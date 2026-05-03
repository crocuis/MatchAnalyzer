import { neon, type NeonQueryFunction } from "@neondatabase/serverless";

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

export type ApiDbClient = any;

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
    private readonly sql: NeonQueryFunction<false, false>,
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

class PostgresClient {
  private readonly sql: NeonQueryFunction<false, false>;

  constructor(databaseUrl: string) {
    this.sql = neon(databaseUrl);
  }

  from(tableName: string): ApiQueryBuilder {
    return new PostgresQueryBuilder(this.sql, tableName);
  }

  async query(text: string, params: unknown[] = []): Promise<ApiDbResult> {
    try {
      const data = await this.sql.query(text, params);
      return { data: data as Record<string, unknown>[], error: null };
    } catch (error) {
      return { data: null, error: normalizeError(error) };
    }
  }
}

export const getDbClient = (
  bindings: AppBindings["Bindings"],
): ApiDbClient | null => {
  const env = getEnv(bindings);

  if (!env.databaseUrl) {
    return null;
  }

  return new PostgresClient(env.databaseUrl);
};
