# Variant Market Copy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 카드에서는 `파생 시장` 표시를 제거하고, 모달에서는 `variantMarkets`를 사람이 읽는 시장 설명으로 바꾼다.

**Architecture:** `MatchCard.tsx`에서 카드용 `variantMarkets` 노출을 제거해 정보 위계를 단순화한다. `PredictionCard.tsx`에는 작은 formatting helper를 추가해 `marketFamily`, `lineValue`, selection/price를 사람이 읽는 제목과 한 줄 설명으로 바꾸고, 테스트와 locale copy를 그 변경에 맞춰 갱신한다.

**Tech Stack:** React, TypeScript, react-i18next, Vitest, Testing Library

---

### Task 1: 카드에서 `variantMarkets` 표시 제거

**Files:**
- Modify: `apps/web/src/components/MatchCard.tsx`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: 카드 테스트를 먼저 RED로 고정**

```tsx
it("does not render derived markets in the card even when variant markets exist", async () => {
  render(<App />);

  const matchButton = await screen.findByRole("button", {
    name: "Liverpool vs Brentford",
  });
  const card = within(matchButton);

  expect(card.queryByText("Derived Markets")).toBeNull();
  expect(card.queryByText("spreads")).toBeNull();
  expect(card.queryByText("totals")).toBeNull();
  expect(card.getByText("Value Pick")).toBeInTheDocument();
});
```

- [ ] **Step 2: RED 확인**

Run: `npm --workspace apps/web run test -- dashboard.test.tsx`

Expected: FAIL because the card still renders `Derived Markets`.

- [ ] **Step 3: 카드 구현을 최소 수정**

```tsx
const hasValuePick = Boolean(match.valueRecommendation?.recommended);
const dateColor =
  predictionPresentation.betState === "recommended"
    ? "var(--accent-primary)"
    : isFinished
      ? "var(--text-muted)"
      : "var(--text-secondary)";

// remove hasVariantMarkets and the trailing metric block entirely
```

삭제 대상:

```tsx
const hasVariantMarkets = Boolean(match.variantMarkets && match.variantMarkets.length > 0);

{hasVariantMarkets ? (
  <div className="matchMetric">
    <span className="metricLabel">{t("matchCard.variantMarkets")}</span>
    <span className="metricValue metricValue-small">
      {match.variantMarkets?.map((market) => market.marketFamily).join(" · ")}
    </span>
  </div>
) : null}
```

- [ ] **Step 4: 카드 테스트 재실행**

Run: `npm --workspace apps/web run test -- dashboard.test.tsx`

Expected: PASS for the new card assertion.

- [ ] **Step 5: 커밋**

```bash
git add apps/web/src/components/MatchCard.tsx apps/web/src/test/dashboard.test.tsx
git commit -m "카드에서 파생시장 표기를 제거한다"
```

### Task 2: 모달용 시장 라벨/설명 helper 추가

**Files:**
- Modify: `apps/web/src/components/PredictionCard.tsx`
- Modify: `apps/web/src/locales/ko.json`
- Modify: `apps/web/src/locales/en.json`

- [ ] **Step 1: formatting helper를 먼저 추가**

```tsx
function humanizeVariantMarketFamily(
  t: ReturnType<typeof useTranslation>["t"],
  marketFamily: string,
) {
  if (marketFamily === "spreads") {
    return t("modal.prediction.variantMarketFamilies.spreads");
  }
  if (marketFamily === "totals") {
    return t("modal.prediction.variantMarketFamilies.totals");
  }
  return marketFamily.replaceAll("_", " ");
}

function summarizeVariantMarket(
  market: VariantMarket,
) {
  const leftPrice = market.selectionAPrice;
  const rightPrice = market.selectionBPrice;
  if (leftPrice !== null && rightPrice !== null) {
    const leadingLabel =
      leftPrice >= rightPrice ? market.selectionALabel : market.selectionBLabel;
    return `${leadingLabel} lead · ${(leftPrice * 100).toFixed(0)}% vs ${(rightPrice * 100).toFixed(0)}%`;
  }
  return `${market.selectionALabel} vs ${market.selectionBLabel}`;
}
```

주의:
- helper는 컴포넌트 파일 안의 작은 순수 함수로 둔다.
- 새 파일 분리는 하지 않는다.

- [ ] **Step 2: 모달 제목과 body copy를 helper 기반으로 교체**

기존:

```tsx
<span className="panelTitle">{t("modal.prediction.variantMarketsTitle")}</span>
...
<strong>{market.marketFamily}</strong>
...
{market.lineValue !== null ? (
  <span style={{ fontWeight: "700" }}>{`L: ${market.lineValue}`}</span>
) : null}
<span>{market.selectionALabel} ...</span>
<span>{market.selectionBLabel} ...</span>
```

교체 후:

```tsx
<span className="panelTitle">{t("modal.prediction.variantMarketsTitle")}</span>
...
<strong>{humanizeVariantMarketFamily(t, market.marketFamily)}</strong>
<span>{summarizeVariantMarket(market)}</span>
```

표현 목표:
- `spreads` -> `Handicap` / `핸디캡`
- `totals` -> `Totals` 또는 `Over/Under` / `언더/오버`
- raw `L: -0.5` 표시는 제거

- [ ] **Step 3: locale copy 갱신**

`apps/web/src/locales/en.json`

```json
"variantMarketsTitle": "Additional markets",
"variantMarketFamilies": {
  "spreads": "Handicap",
  "totals": "Over/Under"
}
```

`apps/web/src/locales/ko.json`

```json
"variantMarketsTitle": "추가 시장",
"variantMarketFamilies": {
  "spreads": "핸디캡",
  "totals": "언더/오버"
}
```

- [ ] **Step 4: 웹 테스트 실행 전 smoke 확인**

Run: `npm --workspace apps/web run test -- dashboard.test.tsx`

Expected: FAIL in modal expectations until test strings are updated.

- [ ] **Step 5: 커밋**

```bash
git add apps/web/src/components/PredictionCard.tsx apps/web/src/locales/en.json apps/web/src/locales/ko.json
git commit -m "모달의 파생시장 표시를 사람이 읽는 설명으로 바꾼다"
```

### Task 3: 회귀 테스트를 새 copy에 맞게 갱신

**Files:**
- Modify: `apps/web/src/test/dashboard.test.tsx`
- Reference: `apps/web/src/components/PredictionCard.tsx`
- Reference: `apps/web/src/components/MatchCard.tsx`

- [ ] **Step 1: 카드/모달 기대값을 새 copy로 갱신**

기존 assertion 예:

```tsx
expect(card.getByText("Derived Markets")).toBeInTheDocument();
expect(screen.getAllByText("Variant markets").length).toBeGreaterThan(0);
expect(screen.getByText("spreads")).toBeInTheDocument();
expect(screen.getByText("L: -0.5")).toBeInTheDocument();
expect(screen.getByText("Home -0.5 (54%)")).toBeInTheDocument();
```

변경 후 예:

```tsx
expect(card.queryByText("Derived Markets")).toBeNull();
expect(modal.getAllByText("Additional markets").length).toBeGreaterThan(0);
expect(screen.getByText("Handicap")).toBeInTheDocument();
expect(screen.getByText("Home -0.5 lead · 54% vs 46%")).toBeInTheDocument();
expect(screen.getByText("Over/Under")).toBeInTheDocument();
expect(screen.getByText("Over 2.5 lead · 57% vs 43%")).toBeInTheDocument();
expect(screen.queryByText("L: -0.5")).toBeNull();
```

- [ ] **Step 2: dashboard test만 실행**

Run: `npm --workspace apps/web run test -- dashboard.test.tsx`

Expected: PASS

- [ ] **Step 3: web workspace 전체 테스트 실행**

Run: `npm --workspace apps/web run test`

Expected: PASS

- [ ] **Step 4: 전체 검증 순서 실행**

Run: `npm test`
Expected: PASS

Run: `npm --workspace apps/api run test`
Expected: PASS

Run: `npm --workspace apps/web run test`
Expected: PASS

Run: `python3 -m pytest`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add apps/web/src/test/dashboard.test.tsx apps/web/src/components/MatchCard.tsx apps/web/src/components/PredictionCard.tsx apps/web/src/locales/en.json apps/web/src/locales/ko.json
git commit -m "파생시장 copy 회귀 테스트를 새 UI 표현에 맞춘다"
```

## Self-Review

- Spec coverage:
  - 카드에서 완전 제거: Task 1
  - 모달에서 시장 종류 + 한 줄 설명: Task 2
  - 번역/테스트 갱신: Task 2, Task 3
  - API payload 변경 제외: plan에 없음
- Placeholder scan:
  - `TODO`, `TBD`, “적절히 처리” 같은 표현 없음
- Type consistency:
  - `variantMarketsTitle`, `variantMarketFamilies`, `humanizeVariantMarketFamily`, `summarizeVariantMarket` 이름을 전 구간에서 동일하게 사용
