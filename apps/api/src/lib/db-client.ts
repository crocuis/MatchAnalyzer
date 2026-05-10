import { Client } from "pg";

import type { AppBindings } from "../env";
import { getEnv } from "../env";

type ApiDbError = { message: string };
type ApiDbResult<TData = Record<string, unknown>[]> = {
  data: TData | null;
  error: ApiDbError | null;
};

type OrderClause = {
  column: string;
  ascending: boolean;
};

type FilterClause = {
  column: string;
  operator: "=" | ">=" | "<" | "in";
  value: unknown;
};

export type ApiQueryBuilder = PromiseLike<ApiDbResult> & {
  select(columns?: string): ApiQueryBuilder;
  eq(column: string, value: unknown): ApiQueryBuilder;
  gte(column: string, value: unknown): ApiQueryBuilder;
  lt(column: string, value: unknown): ApiQueryBuilder;
  in(column: string, values: unknown[]): ApiQueryBuilder;
  order(column: string, options?: { ascending?: boolean }): ApiQueryBuilder;
  limit(count: number): ApiQueryBuilder;
  range(from: number, to: number): ApiQueryBuilder;
  maybeSingle(): PromiseLike<ApiDbResult<Record<string, unknown> | null>>;
};

export type ApiDbClient = {
  from(tableName: string): ApiQueryBuilder;
  query?(text: string, params?: unknown[]): Promise<ApiDbResult>;
  close?(): Promise<void>;
};
export type DbClientFreshness = "cached" | "fresh";
export type GetDbClientOptions = {
  freshness?: DbClientFreshness;
};

function validateIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`invalid SQL identifier: ${value}`);
  }
  return value;
}

function quoteIdentifier(value: string): string {
  return `"${validateIdentifier(value).replaceAll('"', '""')}"`;
}

function parseSelectColumns(columns: string | undefined): string {
  if (!columns || columns.trim() === "*" || columns.trim().length === 0) {
    return "*";
  }

  return columns
    .split(",")
    .map((column) => quoteIdentifier(column.trim()))
    .join(", ");
}

function normalizeError(error: unknown): ApiDbError {
  return {
    message: error instanceof Error ? error.message : String(error),
  };
}

class PostgresQueryBuilder implements ApiQueryBuilder {
  private selectedColumns = "*";
  private readonly filters: FilterClause[] = [];
  private readonly orders: OrderClause[] = [];
  private limitCount: number | null = null;
  private offsetCount: number | null = null;

  constructor(
    private readonly sql: PostgresQueryExecutor,
    private readonly tableName: string,
  ) {}

  select(columns?: string): ApiQueryBuilder {
    this.selectedColumns = parseSelectColumns(columns);
    return this;
  }

  eq(column: string, value: unknown): ApiQueryBuilder {
    this.filters.push({ column, operator: "=", value });
    return this;
  }

  gte(column: string, value: unknown): ApiQueryBuilder {
    this.filters.push({ column, operator: ">=", value });
    return this;
  }

  lt(column: string, value: unknown): ApiQueryBuilder {
    this.filters.push({ column, operator: "<", value });
    return this;
  }

  in(column: string, values: unknown[]): ApiQueryBuilder {
    this.filters.push({ column, operator: "in", value: values });
    return this;
  }

  order(column: string, options?: { ascending?: boolean }): ApiQueryBuilder {
    this.orders.push({
      column,
      ascending: options?.ascending ?? true,
    });
    return this;
  }

  limit(count: number): ApiQueryBuilder {
    this.limitCount = Math.max(0, Math.trunc(count));
    return this;
  }

  range(from: number, to: number): ApiQueryBuilder {
    const normalizedFrom = Math.max(0, Math.trunc(from));
    const normalizedTo = Math.max(normalizedFrom, Math.trunc(to));
    this.offsetCount = normalizedFrom;
    this.limitCount = normalizedTo - normalizedFrom + 1;
    return this;
  }

  maybeSingle(): PromiseLike<ApiDbResult<Record<string, unknown> | null>> {
    this.limit(1);
    return this.execute().then((result) => ({
      data: Array.isArray(result.data) ? result.data[0] ?? null : null,
      error: result.error,
    }));
  }

  then<TResult1 = ApiDbResult, TResult2 = never>(
    onfulfilled?:
      | ((value: ApiDbResult) => TResult1 | PromiseLike<TResult1>)
      | undefined
      | null,
    onrejected?:
      | ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
      | undefined
      | null,
  ): PromiseLike<TResult1 | TResult2> {
    return this.execute().then(onfulfilled, onrejected);
  }

  private buildSql(): { text: string; params: unknown[] } {
    const params: unknown[] = [];
    const whereParts = this.filters.map((filter) => {
      const column = quoteIdentifier(filter.column);
      params.push(filter.value);
      if (filter.operator === "in") {
        return `${column} = ANY($${params.length})`;
      }
      return `${column} ${filter.operator} $${params.length}`;
    });
    const orderBy = this.orders.length > 0
      ? ` order by ${this.orders
          .map(
            (order) =>
              `${quoteIdentifier(order.column)} ${
                order.ascending ? "asc" : "desc"
              }`,
          )
          .join(", ")}`
      : "";
    const limit =
      this.limitCount === null ? "" : ` limit ${Math.max(0, this.limitCount)}`;
    const offset =
      this.offsetCount === null ? "" : ` offset ${Math.max(0, this.offsetCount)}`;
    return {
      text: `select ${this.selectedColumns} from public.${quoteIdentifier(
        this.tableName,
      )}${whereParts.length > 0 ? ` where ${whereParts.join(" and ")}` : ""}${orderBy}${limit}${offset}`,
      params,
    };
  }

  private async execute(): Promise<ApiDbResult> {
    try {
      const query = this.buildSql();
      const data = await this.sql.query(query.text, query.params);
      return { data: data as Record<string, unknown>[], error: null };
    } catch (error) {
      return { data: null, error: normalizeError(error) };
    }
  }
}

type PostgresQueryExecutor = {
  query(text: string, params?: unknown[]): Promise<Record<string, unknown>[]>;
  close(): Promise<void>;
};

const MAX_WORKER_POSTGRES_CONNECTIONS = 5;

class PgQueryExecutor implements PostgresQueryExecutor {
  private connectingClientCount = 0;
  private readonly allClients = new Set<Client>();
  private readonly idleClients: Client[] = [];
  private readonly pendingClientAcquires: Array<(client: Client) => void> = [];

  constructor(private readonly connectionString: string) {}

  async query(
    text: string,
    params: unknown[] = [],
  ): Promise<Record<string, unknown>[]> {
    const client = await this.acquireClient();

    try {
      const result = await client.query(text, params);
      return result.rows as Record<string, unknown>[];
    } finally {
      this.releaseClient(client);
    }
  }

  private async acquireClient(): Promise<Client> {
    const idleClient = this.idleClients.pop();
    if (idleClient) {
      return idleClient;
    }

    if (
      this.allClients.size + this.connectingClientCount
      < MAX_WORKER_POSTGRES_CONNECTIONS
    ) {
      return this.connectClient();
    }

    return new Promise((resolve) => {
      this.pendingClientAcquires.push(resolve);
    });
  }

  private async connectClient(): Promise<Client> {
    this.connectingClientCount += 1;
    const client = new Client({ connectionString: this.connectionString });
    try {
      await client.connect();
      this.allClients.add(client);
      return client;
    } finally {
      this.connectingClientCount = Math.max(0, this.connectingClientCount - 1);
    }
  }

  private releaseClient(client: Client): void {
    const nextAcquire = this.pendingClientAcquires.shift();
    if (nextAcquire) {
      nextAcquire(client);
      return;
    }

    if (this.allClients.has(client)) {
      this.idleClients.push(client);
    }
  }

  async close(): Promise<void> {
    const clients = [...this.allClients];
    this.allClients.clear();
    this.idleClients.length = 0;
    this.pendingClientAcquires.length = 0;
    await Promise.allSettled(clients.map((client) => client.end()));
  }
}

class PostgresClient {
  private readonly sql: PostgresQueryExecutor;

  constructor(databaseUrl: string) {
    this.sql = new PgQueryExecutor(databaseUrl);
  }

  from(tableName: string): ApiQueryBuilder {
    return new PostgresQueryBuilder(this.sql, tableName);
  }

  async query(text: string, params: unknown[] = []): Promise<ApiDbResult> {
    try {
      const data = await this.sql.query(text, params);
      return { data, error: null };
    } catch (error) {
      return { data: null, error: normalizeError(error) };
    }
  }

  async close(): Promise<void> {
    await this.sql.close();
  }
}

export const getDbClient = (
  bindings: AppBindings["Bindings"],
  options: GetDbClientOptions = {},
): ApiDbClient | null => {
  const env = getEnv(bindings);
  const databaseUrl =
    options.freshness === "fresh" ? env.freshDatabaseUrl : env.databaseUrl;

  if (!databaseUrl) {
    return null;
  }

  return new PostgresClient(databaseUrl);
};
