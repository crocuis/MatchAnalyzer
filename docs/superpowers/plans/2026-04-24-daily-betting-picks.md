# 데일리 베팅 추천 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 승무패, 핸디캡, 언더/오버 추천 5~10건을 보여주는 별도 Daily Picks 화면을 만들고, 대시보드에서 자연스럽게 진입하게 한다.

**Architecture:** API에는 `/daily-picks` 전용 라우트를 추가해 기존 match, prediction, variant market 데이터를 추천 카드 형태로 정규화한다. 웹은 전용 fetch 타입과 `DailyPicksView`/`DailyPickCard`를 추가하고, 기존 대시보드 헤더와 요약 CTA에서 화면을 전환한다. 추천 항목은 기존 매치 상세 모달/리포트를 재사용해 근거 확인 흐름을 유지한다.

**Tech Stack:** Hono API, Supabase client abstraction, React, TypeScript, i18next, Vitest, Testing Library, existing CSS system.

---

## 파일 구조

- 생성: `apps/api/src/routes/daily-picks.ts`: Daily Picks API 라우트, query parsing, Supabase row loading, 추천 정규화를 담당한다.
- 수정: `apps/api/src/index.ts`: `app.route("/daily-picks", dailyPicks)`를 등록한다.
- 수정: `apps/api/src/test/api.test.ts`: 빈 응답, 날짜 필터, market-family 필터, 리그 필터, 5~10건 cap을 검증한다.
- 수정: `apps/web/src/lib/api.ts`: `DailyPicksResponse`, `DailyPickItem`, `fetchDailyPicks`를 추가한다.
- 생성: `apps/web/src/components/DailyPicksView.tsx`: 별도 Daily Picks 화면, 필터, 추천 리스트, 로딩/빈/오류 상태를 담당한다.
- 생성: `apps/web/src/components/DailyPickCard.tsx`: 추천 카드의 경기, 시장, 선택지, 신뢰도, 기대값, 가격, 상태 배지를 표시한다.
- 수정: `apps/web/src/App.tsx`: dashboard/dailyPicks view state, 헤더 버튼, 모달 drill-down 연결을 추가한다.
- 수정: `apps/web/src/components/MatchTable.tsx`: 현재 리그 요약 위에 Daily Picks 요약 CTA를 추가한다.
- 수정: `apps/web/src/locales/en.json`, `apps/web/src/locales/ko.json`: 사용자 문구를 추가한다.
- 수정: `apps/web/src/styles.css`: CTA, 필터, 카드, 반응형 레이아웃 스타일을 추가한다.
- 수정: `apps/web/src/test/dashboard.test.tsx`: 진입점, 필터, 카드, 상세 drill-down을 검증한다.

`autoresearch-results/`는 stage하지 않는다. 현재 작업트리에 남아 있는 batch evaluator 파일은 이 UI 작업에서 직접 통합하지 않는 한 그대로 둔다.

---

### Task 1: API 기본 계약과 빈 라우트

**Files:**
- Create: `apps/api/src/routes/daily-picks.ts`
- Modify: `apps/api/src/index.ts`
- Test: `apps/api/src/test/api.test.ts`

- [ ] **Step 1: 실패하는 빈 응답 테스트 작성**

`apps/api/src/test/api.test.ts`에 import를 추가한다.

```ts
import { loadDailyPicksView } from "../routes/daily-picks";
```

같은 파일에 테스트를 추가한다.

```ts
it("returns an empty daily picks payload when no supabase client is configured", async () => {
  const response = await app.request("/daily-picks");

  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({
    generatedAt: null,
    date: null,
    target: {
      minDailyRecommendations: 5,
      maxDailyRecommendations: 10,
      hitRate: 0.7,
      roi: 0.2,
    },
    coverage: {
      moneyline: 0,
      spreads: 0,
      totals: 0,
      held: 0,
    },
    items: [],
    heldItems: [],
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npm --workspace apps/api run test -- daily-picks`

Expected: `../routes/daily-picks`가 없거나 `/daily-picks`가 mount되지 않아 실패한다.

- [ ] **Step 3: 빈 응답 라우트 생성**

`apps/api/src/routes/daily-picks.ts`를 생성한다.

```ts
import { Hono } from "hono";
import type { AppBindings } from "../env";
import { getSupabaseClient, type ApiSupabaseClient } from "../lib/supabase";

const dailyPicks = new Hono<AppBindings>();

export type DailyPickMarketFamily = "moneyline" | "spreads" | "totals";

export type DailyPickItem = {
  id: string;
  matchId: string;
  predictionId: string | null;
  leagueId: string;
  leagueLabel: string;
  homeTeam: string;
  awayTeam: string;
  kickoffAt: string;
  marketFamily: DailyPickMarketFamily;
  selectionLabel: string;
  confidence: number | null;
  edge: number | null;
  expectedValue: number | null;
  marketPrice: number | null;
  modelProbability: number | null;
  marketProbability: number | null;
  sourceAgreementRatio: number | null;
  status: "recommended" | "held" | "pending" | "hit" | "miss";
  noBetReason: string | null;
  reasonLabels: string[];
};

export type DailyPicksView = {
  generatedAt: string | null;
  date: string | null;
  target: {
    minDailyRecommendations: number;
    maxDailyRecommendations: number;
    hitRate: number;
    roi: number;
  };
  coverage: Record<DailyPickMarketFamily | "held", number>;
  items: DailyPickItem[];
  heldItems: DailyPickItem[];
};

const EMPTY_VIEW: DailyPicksView = {
  generatedAt: null,
  date: null,
  target: {
    minDailyRecommendations: 5,
    maxDailyRecommendations: 10,
    hitRate: 0.7,
    roi: 0.2,
  },
  coverage: {
    moneyline: 0,
    spreads: 0,
    totals: 0,
    held: 0,
  },
  items: [],
  heldItems: [],
};

export async function loadDailyPicksView(
  supabase: ApiSupabaseClient | null,
): Promise<DailyPicksView> {
  if (!supabase) {
    return EMPTY_VIEW;
  }
  return EMPTY_VIEW;
}

dailyPicks.get("/", async (c) => {
  const supabase = getSupabaseClient(c.env);
  const view = await loadDailyPicksView(supabase);
  return c.json(view);
});

export default dailyPicks;
```

`apps/api/src/index.ts`에 import와 route를 추가한다.

```ts
import dailyPicks from "./routes/daily-picks";
```

```ts
app.route("/daily-picks", dailyPicks);
```

- [ ] **Step 4: 통과 확인**

Run: `npm --workspace apps/api run test -- daily-picks`

Expected: 빈 응답 테스트가 통과한다.

- [ ] **Step 5: 커밋**

```bash
git add apps/api/src/routes/daily-picks.ts apps/api/src/index.ts apps/api/src/test/api.test.ts
git commit -m "데일리 추천 API의 기본 응답 계약을 추가"
```

---

### Task 2: API 추천 정규화와 필터

**Files:**
- Modify: `apps/api/src/routes/daily-picks.ts`
- Test: `apps/api/src/test/api.test.ts`

- [ ] **Step 1: 추천 생성 테스트 작성**

`apps/api/src/test/api.test.ts`에 fake Supabase helper를 추가한다.

```ts
function buildTableSupabase(tables: Record<string, unknown[]>) {
  return {
    from: vi.fn((tableName: string) => ({
      select: vi.fn().mockReturnThis(),
      order: vi.fn().mockResolvedValue({
        data: tables[tableName] ?? [],
        error: null,
      }),
      limit: vi.fn().mockResolvedValue({
        data: tables[tableName] ?? [],
        error: null,
      }),
    })),
  } as never;
}
```

테스트를 추가한다.

```ts
it("builds capped daily picks across moneyline spreads and totals", async () => {
  const tables = {
    matches: [
      {
        id: "match-1",
        competition_id: "premier-league",
        kickoff_at: "2026-04-24T19:00:00Z",
        home_team_id: "chelsea",
        away_team_id: "man-city",
        final_result: null,
        home_score: null,
        away_score: null,
      },
    ],
    teams: [
      { id: "chelsea", name: "Chelsea", logo_url: null },
      { id: "man-city", name: "Manchester City", logo_url: null },
    ],
    competitions: [
      { id: "premier-league", name: "Premier League", emblem_url: null },
    ],
    match_snapshots: [
      { id: "snapshot-1", match_id: "match-1", checkpoint_type: "T_MINUS_24H" },
    ],
    predictions: [
      {
        id: "prediction-1",
        match_id: "match-1",
        snapshot_id: "snapshot-1",
        recommended_pick: "HOME",
        confidence_score: 0.72,
        main_recommendation_pick: "HOME",
        main_recommendation_confidence: 0.72,
        main_recommendation_recommended: true,
        main_recommendation_no_bet_reason: null,
        value_recommendation_pick: "HOME",
        value_recommendation_recommended: true,
        value_recommendation_edge: 0.12,
        value_recommendation_expected_value: 0.28,
        value_recommendation_market_price: 0.54,
        value_recommendation_model_probability: 0.69,
        value_recommendation_market_probability: 0.57,
        value_recommendation_market_source: "prediction_market",
        variant_markets_summary: [
          {
            market_family: "spreads",
            selection_a_label: "Chelsea -0.5",
            selection_a_price: 0.58,
            selection_b_label: "Manchester City +0.5",
            selection_b_price: 0.42,
            line_value: -0.5,
            source_name: "polymarket_spreads",
          },
          {
            market_family: "totals",
            selection_a_label: "Over 2.5",
            selection_a_price: 0.47,
            selection_b_label: "Under 2.5",
            selection_b_price: 0.53,
            line_value: 2.5,
            source_name: "polymarket_totals",
          },
        ],
        summary_payload: {
          source_agreement_ratio: 0.8,
        },
        explanation_payload: {},
        created_at: "2026-04-24T08:00:00Z",
      },
    ],
  };
  const supabase = buildTableSupabase(tables);

  const view = await loadDailyPicksView(supabase, {
    date: "2026-04-24",
    includeHeld: false,
  });

  expect(view.date).toBe("2026-04-24");
  expect(view.items.map((item) => item.marketFamily)).toEqual([
    "moneyline",
    "spreads",
    "totals",
  ]);
  expect(view.items[0]).toMatchObject({
    matchId: "match-1",
    leagueId: "premier-league",
    homeTeam: "Chelsea",
    awayTeam: "Manchester City",
    marketFamily: "moneyline",
    status: "recommended",
  });
  expect(view.coverage).toMatchObject({
    moneyline: 1,
    spreads: 1,
    totals: 1,
  });
});
```

- [ ] **Step 2: 실패 확인**

Run: `npm --workspace apps/api run test -- daily-picks`

Expected: `loadDailyPicksView`가 아직 빈 응답만 반환해 실패한다.

- [ ] **Step 3: row loading과 option signature 추가**

`apps/api/src/routes/daily-picks.ts`에서 signature와 helper를 추가한다.

```ts
type LoadDailyPicksOptions = {
  date?: string | null;
  leagueId?: string | null;
  marketFamily?: DailyPickMarketFamily | "all" | null;
  includeHeld?: boolean;
};

async function readRows(
  supabase: ApiSupabaseClient,
  tableName: string,
): Promise<Record<string, unknown>[]> {
  const result = await supabase.from(tableName).select("*").order("id");
  if (result.error) {
    throw new Error(result.error.message);
  }
  return Array.isArray(result.data) ? result.data as Record<string, unknown>[] : [];
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}
```

`loadDailyPicksView`를 바꾼다.

```ts
export async function loadDailyPicksView(
  supabase: ApiSupabaseClient | null,
  options: LoadDailyPicksOptions = {},
): Promise<DailyPicksView> {
  if (!supabase) {
    return { ...EMPTY_VIEW, date: options.date ?? null };
  }

  const [matches, teams, competitions, snapshots, predictions] = await Promise.all([
    readRows(supabase, "matches"),
    readRows(supabase, "teams"),
    readRows(supabase, "competitions"),
    readRows(supabase, "match_snapshots"),
    readRows(supabase, "predictions"),
  ]);

  return buildDailyPicksView({
    matches,
    teams,
    competitions,
    snapshots,
    predictions,
    options,
  });
}
```

- [ ] **Step 4: 추천 정규화 구현**

아래 helper들을 `apps/api/src/routes/daily-picks.ts`에 추가한다.

```ts
type BuildDailyPicksArgs = {
  matches: Record<string, unknown>[];
  teams: Record<string, unknown>[];
  competitions: Record<string, unknown>[];
  snapshots: Record<string, unknown>[];
  predictions: Record<string, unknown>[];
  options: LoadDailyPicksOptions;
};

function buildDailyPicksView(args: BuildDailyPicksArgs): DailyPicksView {
  const teamsById = new Map(args.teams.map((row) => [String(row.id), row]));
  const competitionsById = new Map(args.competitions.map((row) => [String(row.id), row]));
  const predictionsByMatch = new Map<string, Record<string, unknown>>();
  for (const prediction of args.predictions) {
    const matchId = readString(prediction.match_id);
    if (!matchId) continue;
    const current = predictionsByMatch.get(matchId);
    if (!current || String(prediction.created_at ?? "") > String(current.created_at ?? "")) {
      predictionsByMatch.set(matchId, prediction);
    }
  }

  const items: DailyPickItem[] = [];
  const heldItems: DailyPickItem[] = [];
  for (const match of args.matches) {
    const kickoffAt = readString(match.kickoff_at);
    if (!kickoffAt || (args.options.date && !kickoffAt.startsWith(args.options.date))) continue;
    const leagueId = readString(match.competition_id) ?? "unknown";
    if (args.options.leagueId && args.options.leagueId !== leagueId) continue;
    const prediction = predictionsByMatch.get(String(match.id));
    if (!prediction) continue;

    const base = buildBasePickContext(match, prediction, teamsById, competitionsById, leagueId);
    for (const pick of buildMoneylineAndVariantPicks(base, prediction)) {
      if (args.options.marketFamily && args.options.marketFamily !== "all" && pick.marketFamily !== args.options.marketFamily) continue;
      if (pick.status === "held") heldItems.push(pick);
      else items.push(pick);
    }
  }

  const sortedItems = items.sort(compareDailyPicks).slice(0, 10);
  const visibleHeldItems = args.options.includeHeld ? heldItems.sort(compareDailyPicks).slice(0, 10) : [];
  return {
    generatedAt: new Date().toISOString(),
    date: args.options.date ?? null,
    target: EMPTY_VIEW.target,
    coverage: {
      moneyline: sortedItems.filter((item) => item.marketFamily === "moneyline").length,
      spreads: sortedItems.filter((item) => item.marketFamily === "spreads").length,
      totals: sortedItems.filter((item) => item.marketFamily === "totals").length,
      held: heldItems.length,
    },
    items: sortedItems,
    heldItems: visibleHeldItems,
  };
}
```

추가 helper를 이어서 넣는다.

```ts
function buildBasePickContext(
  match: Record<string, unknown>,
  prediction: Record<string, unknown>,
  teamsById: Map<string, Record<string, unknown>>,
  competitionsById: Map<string, Record<string, unknown>>,
  leagueId: string,
) {
  const homeTeam = teamsById.get(String(match.home_team_id));
  const awayTeam = teamsById.get(String(match.away_team_id));
  const competition = competitionsById.get(leagueId);
  return {
    matchId: String(match.id),
    predictionId: readString(prediction.id),
    leagueId,
    leagueLabel: readString(competition?.name) ?? leagueId,
    homeTeam: readString(homeTeam?.name) ?? String(match.home_team_id),
    awayTeam: readString(awayTeam?.name) ?? String(match.away_team_id),
    kickoffAt: readString(match.kickoff_at) ?? "",
    sourceAgreementRatio: readNumber((prediction.summary_payload as Record<string, unknown> | undefined)?.source_agreement_ratio),
  };
}

function buildMoneylineAndVariantPicks(
  base: ReturnType<typeof buildBasePickContext>,
  prediction: Record<string, unknown>,
): DailyPickItem[] {
  const status = readBoolean(prediction.main_recommendation_recommended) === false ? "held" : "recommended";
  const moneyline: DailyPickItem = {
    ...base,
    id: `${base.matchId}:moneyline`,
    marketFamily: "moneyline",
    selectionLabel: readString(prediction.main_recommendation_pick) ?? readString(prediction.recommended_pick) ?? "Unavailable",
    confidence: readNumber(prediction.main_recommendation_confidence) ?? readNumber(prediction.confidence_score),
    edge: readNumber(prediction.value_recommendation_edge),
    expectedValue: readNumber(prediction.value_recommendation_expected_value),
    marketPrice: readNumber(prediction.value_recommendation_market_price),
    modelProbability: readNumber(prediction.value_recommendation_model_probability),
    marketProbability: readNumber(prediction.value_recommendation_market_probability),
    sourceAgreementRatio: base.sourceAgreementRatio,
    status,
    noBetReason: readString(prediction.main_recommendation_no_bet_reason),
    reasonLabels: status === "held" ? ["heldByRecommendationGate"] : ["mainRecommendation"],
  };

  const variants = Array.isArray(prediction.variant_markets_summary)
    ? prediction.variant_markets_summary as Record<string, unknown>[]
    : [];
  return [
    moneyline,
    ...variants
      .map((variant) => buildVariantPick(base, variant))
      .filter((item): item is DailyPickItem => item !== null),
  ];
}

function buildVariantPick(
  base: ReturnType<typeof buildBasePickContext>,
  variant: Record<string, unknown>,
): DailyPickItem | null {
  const rawFamily = readString(variant.market_family);
  if (rawFamily !== "spreads" && rawFamily !== "totals") return null;
  const aPrice = readNumber(variant.selection_a_price);
  const bPrice = readNumber(variant.selection_b_price);
  const selectionLabel = (aPrice ?? 0) >= (bPrice ?? 0)
    ? readString(variant.selection_a_label)
    : readString(variant.selection_b_label);
  const marketPrice = Math.max(aPrice ?? 0, bPrice ?? 0);
  return {
    ...base,
    id: `${base.matchId}:${rawFamily}:${selectionLabel ?? "selection"}`,
    marketFamily: rawFamily,
    selectionLabel: selectionLabel ?? "Unavailable",
    confidence: marketPrice > 0 ? Number(marketPrice.toFixed(4)) : null,
    edge: null,
    expectedValue: null,
    marketPrice: marketPrice > 0 ? Number(marketPrice.toFixed(4)) : null,
    modelProbability: null,
    marketProbability: marketPrice > 0 ? Number(marketPrice.toFixed(4)) : null,
    sourceAgreementRatio: base.sourceAgreementRatio,
    status: "recommended",
    noBetReason: null,
    reasonLabels: [rawFamily],
  };
}

function compareDailyPicks(left: DailyPickItem, right: DailyPickItem): number {
  const leftScore = (left.expectedValue ?? 0) + (left.confidence ?? 0);
  const rightScore = (right.expectedValue ?? 0) + (right.confidence ?? 0);
  return rightScore - leftScore || Date.parse(left.kickoffAt) - Date.parse(right.kickoffAt);
}
```

- [ ] **Step 5: query param 연결**

route handler를 아래처럼 수정한다.

```ts
dailyPicks.get("/", async (c) => {
  const supabase = getSupabaseClient(c.env);
  const view = await loadDailyPicksView(supabase, {
    date: c.req.query("date") ?? null,
    leagueId: c.req.query("leagueId") ?? null,
    marketFamily: (c.req.query("marketFamily") ?? "all") as DailyPickMarketFamily | "all",
    includeHeld: c.req.query("includeHeld") === "true",
  });
  return c.json(view, 200, {
    "cache-control": "public, max-age=30, s-maxage=30, stale-while-revalidate=120",
  });
});
```

- [ ] **Step 6: API 테스트 실행**

Run: `npm --workspace apps/api run test -- daily-picks`

Expected: daily-picks 관련 테스트가 모두 통과한다.

- [ ] **Step 7: 커밋**

```bash
git add apps/api/src/routes/daily-picks.ts apps/api/src/test/api.test.ts
git commit -m "데일리 추천 API의 추천 정규화를 구현"
```

---

### Task 3: 웹 API 타입과 fetcher

**Files:**
- Modify: `apps/web/src/lib/api.ts`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: fetcher 테스트 작성**

`apps/web/src/test/dashboard.test.tsx`에 import를 추가한다.

```ts
import { fetchDailyPicks } from "../lib/api";
```

테스트를 추가한다.

```ts
it("fetches daily picks with filters", async () => {
  const fetchMock = vi.fn(async () => ({
    ok: true,
    json: async () => ({
      generatedAt: "2026-04-24T08:00:00Z",
      date: "2026-04-24",
      target: { minDailyRecommendations: 5, maxDailyRecommendations: 10, hitRate: 0.7, roi: 0.2 },
      coverage: { moneyline: 1, spreads: 1, totals: 1, held: 1 },
      items: [],
      heldItems: [],
    }),
  }));
  vi.stubGlobal("fetch", fetchMock);

  await fetchDailyPicks({
    date: "2026-04-24",
    leagueId: "premier-league",
    marketFamily: "spreads",
    includeHeld: true,
  });

  expect(fetchMock).toHaveBeenCalledWith(
    "/api/daily-picks?date=2026-04-24&leagueId=premier-league&marketFamily=spreads&includeHeld=true",
  );
});
```

- [ ] **Step 2: 실패 확인**

Run: `npm --workspace apps/web run test -- daily`

Expected: `fetchDailyPicks` export가 없어 실패한다.

- [ ] **Step 3: 타입과 fetcher 추가**

`apps/web/src/lib/api.ts`에 추가한다.

```ts
export type DailyPickMarketFamily = "moneyline" | "spreads" | "totals";
export type DailyPickStatus = "recommended" | "held" | "pending" | "hit" | "miss";

export interface DailyPickItem {
  id: string;
  matchId: string;
  predictionId: string | null;
  leagueId: string;
  leagueLabel: string;
  homeTeam: string;
  awayTeam: string;
  kickoffAt: string;
  marketFamily: DailyPickMarketFamily;
  selectionLabel: string;
  confidence: number | null;
  edge: number | null;
  expectedValue: number | null;
  marketPrice: number | null;
  modelProbability: number | null;
  marketProbability: number | null;
  sourceAgreementRatio: number | null;
  status: DailyPickStatus;
  noBetReason: string | null;
  reasonLabels: string[];
}

export interface DailyPicksResponse {
  generatedAt: string | null;
  date: string | null;
  target: {
    minDailyRecommendations: number;
    maxDailyRecommendations: number;
    hitRate: number;
    roi: number;
  };
  coverage: Record<DailyPickMarketFamily | "held", number>;
  items: DailyPickItem[];
  heldItems: DailyPickItem[];
}

export function fetchDailyPicks(params?: {
  date?: string | null;
  leagueId?: string | null;
  marketFamily?: DailyPickMarketFamily | "all";
  includeHeld?: boolean;
}): Promise<DailyPicksResponse> {
  const search = new URLSearchParams();
  if (params?.date) search.set("date", params.date);
  if (params?.leagueId) search.set("leagueId", params.leagueId);
  if (params?.marketFamily && params.marketFamily !== "all") {
    search.set("marketFamily", params.marketFamily);
  }
  if (params?.includeHeld) search.set("includeHeld", "true");
  const query = search.toString();
  return fetchJson<DailyPicksResponse>(query ? `/daily-picks?${query}` : "/daily-picks");
}
```

- [ ] **Step 4: 웹 테스트 실행**

Run: `npm --workspace apps/web run test -- daily`

Expected: fetcher 테스트가 통과한다.

- [ ] **Step 5: 커밋**

```bash
git add apps/web/src/lib/api.ts apps/web/src/test/dashboard.test.tsx
git commit -m "웹 클라이언트에 데일리 추천 API 계약을 추가"
```

---

### Task 4: 대시보드 진입점

**Files:**
- Modify: `apps/web/src/App.tsx`
- Modify: `apps/web/src/components/MatchTable.tsx`
- Modify: `apps/web/src/locales/en.json`
- Modify: `apps/web/src/locales/ko.json`
- Modify: `apps/web/src/styles.css`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: 진입점 테스트 작성**

테스트를 추가한다.

```ts
it("shows daily picks header and board entry points", async () => {
  render(<App />);

  expect(await screen.findByRole("button", { name: /daily picks/i })).toBeInTheDocument();
  expect(await screen.findByRole("button", { name: /open daily picks/i })).toBeInTheDocument();
  expect(screen.getByText(/qualified recommendations/i)).toBeInTheDocument();
});

it("opens the daily picks view from the dashboard CTA", async () => {
  render(<App />);

  fireEvent.click(await screen.findByRole("button", { name: /open daily picks/i }));

  expect(await screen.findByRole("heading", { name: /daily picks/i })).toBeInTheDocument();
});
```

- [ ] **Step 2: 실패 확인**

Run: `npm --workspace apps/web run test -- daily`

Expected: 버튼과 view state가 없어 실패한다.

- [ ] **Step 3: App view state와 헤더 버튼 추가**

`apps/web/src/App.tsx`에 import를 추가한다.

```ts
import DailyPicksView from "./components/DailyPicksView";
```

state를 추가한다.

```ts
const [activeView, setActiveView] = useState<"dashboard" | "dailyPicks">("dashboard");
const [dailyPicksLeagueId, setDailyPicksLeagueId] = useState<string | null>(null);
```

기존 dashboard return 전에 Daily Picks view block을 추가한다.

```tsx
if (activeView === "dailyPicks") {
  return (
    <main className="dashboardApp">
      <div className="dashboardShell">
        <DailyPicksView
          initialLeagueId={dailyPicksLeagueId}
          leagues={derivedLeagues}
          onBack={() => setActiveView("dashboard")}
          onOpenMatch={handleOpenMatch}
        />
        <MatchDetailModal
          match={activeMatch}
          isOpen={isModalOpen}
          onClose={handleCloseModal}
          onOpenReport={handleOpenReport}
          prediction={activeDetail?.prediction ?? null}
          checkpoints={activeDetail?.checkpoints ?? []}
          review={activeDetail?.review ?? null}
        />
      </div>
    </main>
  );
}
```

`.dashboardHeader` 안에 버튼을 추가한다.

```tsx
<button
  className="dailyPicksHeaderButton"
  type="button"
  onClick={() => {
    setDailyPicksLeagueId(null);
    setActiveView("dailyPicks");
  }}
>
  {t("dailyPicks.entry.header")}
</button>
```

- [ ] **Step 4: MatchTable CTA prop 추가**

`MatchTableProps`에 추가한다.

```ts
onOpenDailyPicks?: (leagueId: string | null) => void;
```

`predictionSummaryBanner` 앞에 CTA를 렌더링한다.

```tsx
<section className="dailyPicksTeaser" aria-label={t("dailyPicks.entry.boardTitle")}>
  <div>
    <span className="metricLabel">{t("dailyPicks.entry.eyebrow")}</span>
    <h2>{t("dailyPicks.entry.boardTitle")}</h2>
    <p>{t("dailyPicks.entry.boardCaption")}</p>
  </div>
  <div className="dailyPicksTeaserStats">
    <span>{t("dailyPicks.entry.volume")}</span>
    <strong>{t("dailyPicks.entry.volumeValue")}</strong>
  </div>
  <button
    className="dailyPicksPrimaryButton"
    type="button"
    onClick={() => onOpenDailyPicks?.(null)}
  >
    {t("dailyPicks.entry.open")}
  </button>
</section>
```

`App.tsx`에서 prop을 넘긴다.

```tsx
onOpenDailyPicks={(leagueId) => {
  setDailyPicksLeagueId(leagueId);
  setActiveView("dailyPicks");
}}
```

- [ ] **Step 5: locale 문구 추가**

`en.json`에 추가한다.

```json
"dailyPicks": {
  "entry": {
    "header": "Daily Picks",
    "eyebrow": "Betting board",
    "boardTitle": "Today's qualified recommendations",
    "boardCaption": "Moneyline, handicap, and over/under picks filtered by confidence and expected value.",
    "volume": "Daily volume",
    "volumeValue": "5-10 picks",
    "open": "Open Daily Picks"
  }
}
```

`ko.json`에 추가한다.

```json
"dailyPicks": {
  "entry": {
    "header": "Daily Picks",
    "eyebrow": "추천 보드",
    "boardTitle": "오늘의 조건 통과 추천",
    "boardCaption": "승무패, 핸디캡, 언더/오버를 신뢰도와 기대값 기준으로 선별합니다.",
    "volume": "하루 추천 수",
    "volumeValue": "5-10건",
    "open": "Daily Picks 열기"
  }
}
```

- [ ] **Step 6: CTA 스타일 추가**

`apps/web/src/styles.css`에 추가한다.

```css
.dailyPicksHeaderButton,
.dailyPicksPrimaryButton {
  border: 0;
  border-radius: 8px;
  background: var(--accent-primary);
  color: #fff;
  font-weight: 800;
  padding: 10px 14px;
  cursor: pointer;
}

.dailyPicksTeaser {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto auto;
  gap: 16px;
  align-items: center;
  margin-bottom: 20px;
  padding: 18px;
  border: 1px solid var(--border-subtle);
  background: var(--surface-primary);
  border-radius: 8px;
}

.dailyPicksTeaser h2 {
  margin: 4px 0;
}

.dailyPicksTeaser p {
  margin: 0;
  color: var(--text-secondary);
}

.dailyPicksTeaserStats {
  display: grid;
  gap: 4px;
  min-width: 112px;
}

@media (max-width: 760px) {
  .dailyPicksTeaser {
    grid-template-columns: 1fr;
  }
}
```

- [ ] **Step 7: Task 5와 함께 테스트 실행**

Run: `npm --workspace apps/web run test -- daily`

Expected: `DailyPicksView`가 Task 5에서 생성된 뒤 진입점 테스트가 통과한다.

- [ ] **Step 8: 커밋 시점**

`DailyPicksView` import가 생기므로 Task 5까지 끝낸 뒤 함께 커밋한다.

---

### Task 5: Daily Picks 화면과 카드

**Files:**
- Create: `apps/web/src/components/DailyPicksView.tsx`
- Create: `apps/web/src/components/DailyPickCard.tsx`
- Modify: `apps/web/src/locales/en.json`
- Modify: `apps/web/src/locales/ko.json`
- Modify: `apps/web/src/styles.css`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: daily-picks fetch mock 추가**

`dashboard.test.tsx`의 fetch stub에 추가한다.

```ts
if (url.startsWith("/api/daily-picks")) {
  return {
    ok: true,
    json: async () => ({
      generatedAt: "2026-04-24T08:00:00Z",
      date: "2026-04-24",
      target: { minDailyRecommendations: 5, maxDailyRecommendations: 10, hitRate: 0.7, roi: 0.2 },
      coverage: { moneyline: 1, spreads: 1, totals: 1, held: 1 },
      items: [
        {
          id: "pick-1",
          matchId: "match-001",
          predictionId: "prediction-1",
          leagueId: "premier-league",
          leagueLabel: "Premier League",
          homeTeam: "Chelsea",
          awayTeam: "Manchester City",
          kickoffAt: "2026-04-27 19:00 UTC",
          marketFamily: "moneyline",
          selectionLabel: "HOME",
          confidence: 0.72,
          edge: 0.12,
          expectedValue: 0.28,
          marketPrice: 0.54,
          modelProbability: 0.69,
          marketProbability: 0.57,
          sourceAgreementRatio: 0.8,
          status: "recommended",
          noBetReason: null,
          reasonLabels: ["mainRecommendation"],
        },
        {
          id: "pick-2",
          matchId: "match-002",
          predictionId: "prediction-2",
          leagueId: "premier-league",
          leagueLabel: "Premier League",
          homeTeam: "Liverpool",
          awayTeam: "Brentford",
          kickoffAt: "2026-04-27 21:00 UTC",
          marketFamily: "spreads",
          selectionLabel: "Home -0.5",
          confidence: 0.69,
          edge: 0.08,
          expectedValue: 0.18,
          marketPrice: 0.58,
          modelProbability: 0.66,
          marketProbability: 0.58,
          sourceAgreementRatio: 0.75,
          status: "recommended",
          noBetReason: null,
          reasonLabels: ["spreads"],
        },
        {
          id: "pick-3",
          matchId: "match-003",
          predictionId: "prediction-3",
          leagueId: "champions-league",
          leagueLabel: "UEFA Champions League",
          homeTeam: "Inter",
          awayTeam: "Bayern Munich",
          kickoffAt: "2026-04-28 19:00 UTC",
          marketFamily: "totals",
          selectionLabel: "Under 2.5",
          confidence: 0.71,
          edge: 0.1,
          expectedValue: 0.21,
          marketPrice: 0.53,
          modelProbability: 0.64,
          marketProbability: 0.53,
          sourceAgreementRatio: 0.7,
          status: "recommended",
          noBetReason: null,
          reasonLabels: ["totals"],
        },
      ],
      heldItems: [
        {
          id: "held-1",
          matchId: "match-004",
          predictionId: "prediction-4",
          leagueId: "premier-league",
          leagueLabel: "Premier League",
          homeTeam: "Arsenal",
          awayTeam: "Fulham",
          kickoffAt: "2026-04-20 19:00 UTC",
          marketFamily: "moneyline",
          selectionLabel: "HOME",
          confidence: 0.51,
          edge: 0.02,
          expectedValue: 0.04,
          marketPrice: 0.61,
          modelProbability: 0.63,
          marketProbability: 0.61,
          sourceAgreementRatio: 0.5,
          status: "held",
          noBetReason: "low_confidence",
          reasonLabels: ["heldByRecommendationGate"],
        },
      ],
    }),
  };
}
```

- [ ] **Step 2: 화면 테스트 작성**

테스트를 추가한다.

```ts
it("renders daily picks market filters and recommendation cards", async () => {
  render(<App />);

  fireEvent.click(await screen.findByRole("button", { name: /open daily picks/i }));

  expect(await screen.findByRole("heading", { name: /daily picks/i })).toBeInTheDocument();
  expect(screen.getByText("HOME")).toBeInTheDocument();
  expect(screen.getByText("Home -0.5")).toBeInTheDocument();
  expect(screen.getByText("Under 2.5")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: /handicap/i })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: /over\/under/i })).toBeInTheDocument();
});

it("filters daily picks by market family and can show held items", async () => {
  render(<App />);

  fireEvent.click(await screen.findByRole("button", { name: /open daily picks/i }));
  fireEvent.click(await screen.findByRole("button", { name: /handicap/i }));

  expect(screen.getByText("Home -0.5")).toBeInTheDocument();
  expect(screen.queryByText("Under 2.5")).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("switch", { name: /show held/i }));

  expect(await screen.findByText(/low confidence/i)).toBeInTheDocument();
});
```

- [ ] **Step 3: 실패 확인**

Run: `npm --workspace apps/web run test -- daily`

Expected: 컴포넌트가 없어 실패한다.

- [ ] **Step 4: `DailyPickCard` 생성**

`apps/web/src/components/DailyPickCard.tsx`를 생성한다.

```tsx
import { useTranslation } from "react-i18next";
import type { DailyPickItem } from "../lib/api";

type DailyPickCardProps = {
  item: DailyPickItem;
  onOpenMatch: (matchId: string) => void;
};

function formatPercent(value: number | null): string {
  return value === null ? "—" : `${Math.round(value * 100)}%`;
}

function formatSignedPercent(value: number | null): string {
  if (value === null) return "—";
  const sign = value > 0 ? "+" : "";
  return `${sign}${Math.round(value * 100)}%`;
}

export default function DailyPickCard({ item, onOpenMatch }: DailyPickCardProps) {
  const { t } = useTranslation();
  const statusLabel = item.status === "held"
    ? t("dailyPicks.status.held")
    : t("dailyPicks.status.recommended");

  return (
    <article className={`dailyPickCard dailyPickCard-${item.status}`}>
      <button
        className="dailyPickCardButton"
        type="button"
        onClick={() => onOpenMatch(item.matchId)}
      >
        <span className="dailyPickLeague">{item.leagueLabel}</span>
        <strong className="dailyPickMatch">{item.homeTeam} vs {item.awayTeam}</strong>
        <span className="dailyPickKickoff">{item.kickoffAt}</span>
      </button>
      <div className="dailyPickDecision">
        <span className="dailyPickFamily">{t(`dailyPicks.marketFamilies.${item.marketFamily}`)}</span>
        <strong>{item.selectionLabel}</strong>
        <span className="dailyPickStatus">{statusLabel}</span>
      </div>
      <div className="dailyPickMetrics">
        <span><small>{t("dailyPicks.metrics.confidence")}</small><strong>{formatPercent(item.confidence)}</strong></span>
        <span><small>{t("dailyPicks.metrics.expectedValue")}</small><strong>{formatSignedPercent(item.expectedValue)}</strong></span>
        <span><small>{t("dailyPicks.metrics.marketPrice")}</small><strong>{formatPercent(item.marketPrice)}</strong></span>
        <span><small>{t("dailyPicks.metrics.modelProbability")}</small><strong>{formatPercent(item.modelProbability)}</strong></span>
      </div>
      {item.noBetReason ? (
        <p className="dailyPickReason">
          {t(`dailyPicks.noBetReasons.${item.noBetReason}`, item.noBetReason)}
        </p>
      ) : null}
    </article>
  );
}
```

- [ ] **Step 5: `DailyPicksView` 생성**

`apps/web/src/components/DailyPicksView.tsx`를 생성한다.

```tsx
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  fetchDailyPicks,
  type DailyPickMarketFamily,
  type DailyPicksResponse,
  type LeagueSummary,
} from "../lib/api";
import DailyPickCard from "./DailyPickCard";

type DailyPicksViewProps = {
  initialLeagueId: string | null;
  leagues: LeagueSummary[];
  onBack: () => void;
  onOpenMatch: (matchId: string) => void;
};

type MarketFilter = "all" | DailyPickMarketFamily;

const MARKET_FILTERS: MarketFilter[] = ["all", "moneyline", "spreads", "totals"];

export default function DailyPicksView({
  initialLeagueId,
  leagues,
  onBack,
  onOpenMatch,
}: DailyPicksViewProps) {
  const { t } = useTranslation();
  const [marketFamily, setMarketFamily] = useState<MarketFilter>("all");
  const [leagueId, setLeagueId] = useState<string | null>(initialLeagueId);
  const [includeHeld, setIncludeHeld] = useState(false);
  const [status, setStatus] = useState<"loading" | "ready" | "error">("loading");
  const [payload, setPayload] = useState<DailyPicksResponse | null>(null);

  useEffect(() => {
    let isMounted = true;
    setStatus("loading");
    void fetchDailyPicks({ leagueId, marketFamily, includeHeld })
      .then((response) => {
        if (!isMounted) return;
        setPayload(response);
        setStatus("ready");
      })
      .catch(() => {
        if (!isMounted) return;
        setPayload(null);
        setStatus("error");
      });
    return () => {
      isMounted = false;
    };
  }, [includeHeld, leagueId, marketFamily]);

  const visibleItems = useMemo(
    () => payload ? [...payload.items, ...(includeHeld ? payload.heldItems : [])] : [],
    [includeHeld, payload],
  );

  return (
    <section className="dailyPicksView" aria-labelledby="daily-picks-heading">
      <button className="dailyPicksBackButton" type="button" onClick={onBack}>
        {t("dailyPicks.back")}
      </button>
      <header className="dailyPicksHero">
        <span className="dashboardEyebrow">{t("dailyPicks.entry.eyebrow")}</span>
        <h1 id="daily-picks-heading">{t("dailyPicks.title")}</h1>
        <p>{t("dailyPicks.subtitle")}</p>
        <div className="dailyPicksTargetGrid">
          <span><small>{t("dailyPicks.target.hitRate")}</small><strong>{Math.round((payload?.target.hitRate ?? 0.7) * 100)}%</strong></span>
          <span><small>{t("dailyPicks.target.roi")}</small><strong>{Math.round((payload?.target.roi ?? 0.2) * 100)}%</strong></span>
          <span><small>{t("dailyPicks.target.volume")}</small><strong>{payload?.target.minDailyRecommendations ?? 5}-{payload?.target.maxDailyRecommendations ?? 10}</strong></span>
        </div>
      </header>

      <div className="dailyPicksFilters">
        {MARKET_FILTERS.map((family) => (
          <button
            className={marketFamily === family ? "dailyPicksFilter-active" : ""}
            key={family}
            type="button"
            onClick={() => setMarketFamily(family)}
          >
            {t(`dailyPicks.marketFamilies.${family}`)}
          </button>
        ))}
        <select
          aria-label={t("dailyPicks.filters.league")}
          value={leagueId ?? ""}
          onChange={(event) => setLeagueId(event.target.value || null)}
        >
          <option value="">{t("dailyPicks.filters.allLeagues")}</option>
          {leagues.map((league) => (
            <option key={league.id} value={league.id}>{league.label}</option>
          ))}
        </select>
        <label className="dailyPicksHeldToggle">
          <input
            aria-label={t("dailyPicks.filters.showHeld")}
            checked={includeHeld}
            role="switch"
            type="checkbox"
            onChange={(event) => setIncludeHeld(event.target.checked)}
          />
          {t("dailyPicks.filters.showHeld")}
        </label>
      </div>

      {status === "loading" ? <p className="timelineNote">{t("status.loading")}</p> : null}
      {status === "error" ? <p className="timelineNote">{t("dailyPicks.error")}</p> : null}
      {status === "ready" && visibleItems.length === 0 ? (
        <p className="timelineNote">{t("dailyPicks.empty")}</p>
      ) : null}
      {status === "ready" && visibleItems.length > 0 ? (
        <div className="dailyPicksList">
          {visibleItems.map((item) => (
            <DailyPickCard item={item} key={item.id} onOpenMatch={onOpenMatch} />
          ))}
        </div>
      ) : null}
    </section>
  );
}
```

- [ ] **Step 6: locale 문구 확장**

Task 4의 `dailyPicks` 객체에 아래 필드를 추가한다.

`en.json`:

```json
"title": "Daily Picks",
"subtitle": "A focused board of today's qualified recommendations. Historical targets are shown as model gates, not future profit guarantees.",
"back": "Back to dashboard",
"empty": "No recommendations passed today's filters.",
"error": "Daily Picks could not be loaded.",
"marketFamilies": {
  "all": "All",
  "moneyline": "Moneyline",
  "spreads": "Handicap",
  "totals": "Over/Under"
},
"filters": {
  "league": "League",
  "allLeagues": "All leagues",
  "showHeld": "Show held"
},
"target": {
  "hitRate": "Backtest hit rate",
  "roi": "Backtest ROI",
  "volume": "Daily picks"
},
"metrics": {
  "confidence": "Confidence",
  "expectedValue": "Expected value",
  "marketPrice": "Market price",
  "modelProbability": "Model probability"
},
"status": {
  "recommended": "Recommended",
  "held": "Held"
},
"noBetReasons": {
  "low_confidence": "Low confidence",
  "heldByRecommendationGate": "Held by recommendation gate"
}
```

`ko.json`:

```json
"title": "Daily Picks",
"subtitle": "오늘 조건을 통과한 추천만 모아 봅니다. 성과 기준은 백테스트 목표이며 미래 수익 보장이 아닙니다.",
"back": "대시보드로 돌아가기",
"empty": "오늘 조건을 통과한 추천이 없습니다.",
"error": "Daily Picks를 불러오지 못했습니다.",
"marketFamilies": {
  "all": "전체",
  "moneyline": "승무패",
  "spreads": "핸디캡",
  "totals": "언더/오버"
},
"filters": {
  "league": "리그",
  "allLeagues": "전체 리그",
  "showHeld": "보류 항목 표시"
},
"target": {
  "hitRate": "백테스트 적중률",
  "roi": "백테스트 ROI",
  "volume": "하루 추천 수"
},
"metrics": {
  "confidence": "신뢰도",
  "expectedValue": "기대값",
  "marketPrice": "시장 가격",
  "modelProbability": "모델 확률"
},
"status": {
  "recommended": "추천",
  "held": "보류"
},
"noBetReasons": {
  "low_confidence": "신뢰도 부족",
  "heldByRecommendationGate": "추천 게이트 보류"
}
```

- [ ] **Step 7: 화면 스타일 추가**

`apps/web/src/styles.css`에 추가한다.

```css
.dailyPicksView {
  display: grid;
  gap: 20px;
}

.dailyPicksBackButton {
  justify-self: start;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--surface-primary);
  color: var(--text-primary);
  padding: 8px 12px;
  font-weight: 700;
}

.dailyPicksHero {
  display: grid;
  gap: 12px;
}

.dailyPicksHero h1 {
  margin: 0;
  font-size: 2rem;
}

.dailyPicksHero p {
  margin: 0;
  color: var(--text-secondary);
  max-width: 720px;
}

.dailyPicksTargetGrid,
.dailyPickMetrics {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.dailyPicksTargetGrid span,
.dailyPickMetrics span {
  display: grid;
  gap: 4px;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--surface-primary);
  padding: 10px;
}

.dailyPicksFilters {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}

.dailyPicksFilters button,
.dailyPicksFilters select {
  min-height: 40px;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--surface-primary);
  color: var(--text-primary);
  padding: 0 12px;
  font-weight: 700;
}

.dailyPicksFilter-active {
  border-color: var(--accent-primary) !important;
  color: var(--accent-primary) !important;
}

.dailyPicksHeldToggle {
  display: inline-flex;
  gap: 8px;
  align-items: center;
  color: var(--text-secondary);
  font-weight: 700;
}

.dailyPicksList {
  display: grid;
  gap: 12px;
}

.dailyPickCard {
  display: grid;
  grid-template-columns: minmax(220px, 1fr) minmax(160px, .7fr) minmax(320px, 1.2fr);
  gap: 14px;
  align-items: center;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--surface-primary);
  padding: 14px;
}

.dailyPickCard-held {
  opacity: .72;
}

.dailyPickCardButton {
  display: grid;
  gap: 4px;
  text-align: left;
  border: 0;
  background: transparent;
  color: inherit;
  padding: 0;
  cursor: pointer;
}

.dailyPickLeague,
.dailyPickKickoff,
.dailyPickMetrics small,
.dailyPickFamily {
  color: var(--text-secondary);
  font-size: .78rem;
}

.dailyPickMatch,
.dailyPickDecision strong {
  color: var(--text-primary);
}

.dailyPickDecision {
  display: grid;
  gap: 4px;
}

.dailyPickStatus {
  color: var(--accent-primary);
  font-weight: 800;
}

.dailyPickReason {
  grid-column: 1 / -1;
  margin: 0;
  color: var(--text-secondary);
}

@media (max-width: 920px) {
  .dailyPickCard,
  .dailyPicksTargetGrid,
  .dailyPickMetrics {
    grid-template-columns: 1fr;
  }
}
```

- [ ] **Step 8: 웹 테스트 실행**

Run: `npm --workspace apps/web run test -- daily`

Expected: Daily Picks 화면 테스트가 통과한다.

- [ ] **Step 9: 커밋**

```bash
git add apps/web/src/components/DailyPicksView.tsx apps/web/src/components/DailyPickCard.tsx apps/web/src/App.tsx apps/web/src/components/MatchTable.tsx apps/web/src/locales/en.json apps/web/src/locales/ko.json apps/web/src/styles.css apps/web/src/test/dashboard.test.tsx
git commit -m "데일리 추천 화면과 대시보드 진입점을 추가"
```

---

### Task 6: 상세 진입과 최종 검증

**Files:**
- Modify: `apps/web/src/components/DailyPicksView.tsx`
- Modify: `apps/web/src/test/dashboard.test.tsx`
- Review: prior tasks changed files

- [ ] **Step 1: 상세 진입 회귀 테스트 작성**

테스트를 추가한다.

```ts
it("opens match detail from a daily pick", async () => {
  render(<App />);

  fireEvent.click(await screen.findByRole("button", { name: /open daily picks/i }));
  fireEvent.click(await screen.findByRole("button", { name: /Chelsea vs Manchester City/i }));

  await waitFor(() => {
    expect(screen.getByText(/Chelsea vs Manchester City/i)).toBeInTheDocument();
  });
  expect(screen.getByText(/advanced prediction/i)).toBeInTheDocument();
});
```

- [ ] **Step 2: 상세 진입 테스트 실행**

Run: `npm --workspace apps/web run test -- opens\\ match\\ detail`

Expected: `DailyPickCard`가 올바른 match id로 `onOpenMatch`를 호출하면 통과한다. 기존 모달 테스트가 role 기반으로 되어 있으면 같은 pattern을 사용해 assertion을 맞춘다.

- [ ] **Step 3: 필수 검증 실행**

아래 순서로 실행한다.

```bash
git status --short
git diff --stat
git diff --check
npm test
npm --workspace apps/api run test
npm --workspace apps/web run test
python3 -m pytest
```

Expected:
- `git diff --check` 출력 없음.
- 모든 테스트 명령 종료 코드 `0`.
- skip, xfail, warning은 최종 보고에 분리해서 기록.

- [ ] **Step 4: 최종 보강 커밋**

Task 6에서 파일 변경이 있으면 커밋한다.

```bash
git add apps/web/src/components/DailyPicksView.tsx apps/web/src/test/dashboard.test.tsx
git commit -m "데일리 추천 상세 진입 회귀 테스트를 보강"
```

- [ ] **Step 5: 최종 구현 보고**

보고에 포함한다.
- 변경 파일
- 실행한 테스트
- historical variant-market coverage 부족 리스크
- 70% 적중률과 20% ROI는 백테스트 목표이며 미래 수익 보장이 아니라는 UI 문구 제약

---

## 자체 검토

Spec coverage:
- 별도 Daily Picks 화면: Task 4, Task 5.
- 대시보드 헤더와 CTA 진입점: Task 4.
- 시장 유형 필터와 보류 토글: Task 5.
- 신뢰도, 기대값, 가격, 모델 확률을 담은 추천 카드: Task 5.
- 날짜, 리그, 시장 유형, 보류 포함 필터 API: Task 1, Task 2.
- 기존 매치 상세로 drill-down: Task 6.
- 로딩, 빈 상태, 오류, 커버리지 요약: Task 2, Task 5.
- API/프론트/회귀 테스트: Task 1~6.

Plan quality checks:
- 열린 결정 항목 없이 각 task가 파일, 코드, 명령, 기대 결과를 가진다.
- API 응답 type과 웹 client type 이름을 일치시켰다.
- 기존 미커밋 작업과 `autoresearch-results/`를 건드리지 않는 전제로 작성했다.
