# Dashboard Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 경기 목록 스캔, 리뷰 필요 상태, 분석 모달, 전체 리포트 흐름이 읽기 쉽게 드러나는 운영형 대시보드로 `apps/web`를 재구성한다.

**Architecture:** 기존 `App` 중심 샘플 셸을 `리그 탭 + 카드 그리드 + 분석 모달 + 전체 리포트 뷰` 구조로 분해한다. 새 의존성 없이 React 상태와 현재 컴포넌트 조합만으로 구현하고, 상세 페이지는 별도 라우터 대신 앱 내부의 보고서 뷰로 시작한다.

**Tech Stack:** React, TypeScript, Vite, Vitest, Testing Library

---

## 파일 구조

- Modify: `apps/web/src/App.tsx`
  - 리그 선택, 경기 선택, 모달 열기/닫기, 리포트 뷰 전환을 관리한다.
- Modify: `apps/web/src/lib/api.ts`
  - 카드/탭/리포트 뷰가 읽기 쉬운 형태로 쓸 수 있는 타입을 추가한다.
- Modify: `apps/web/src/components/MatchTable.tsx`
  - 표 렌더링을 중단하고, 경기 카드 그리드의 상위 컨테이너로 전환한다.
- Modify: `apps/web/src/components/PredictionCard.tsx`
  - 큰 추천 픽 + confidence + 확률 바 블록으로 재구성한다.
- Modify: `apps/web/src/components/CheckpointTimeline.tsx`
  - 모달에서는 요약형, 리포트에서는 확장형으로 읽히게 조정한다.
- Modify: `apps/web/src/components/PostMatchReviewCard.tsx`
  - 리뷰 필요 상태와 결과 요약을 읽기 쉽게 정리한다.
- Create: `apps/web/src/components/LeagueTabs.tsx`
  - 텍스트 탭 + 밑줄 강조 + 보조 메타 줄을 담당한다.
- Create: `apps/web/src/components/MatchCard.tsx`
  - 개별 경기 카드 UI와 `Needs Review` 좌측 액센트 바를 담당한다.
- Create: `apps/web/src/components/MatchDetailModal.tsx`
  - 단일 컬럼 분석 모달을 담당한다.
- Create: `apps/web/src/components/ProbabilityBars.tsx`
  - 예측 확률을 숫자 + 가로 막대 조합으로 렌더링한다.
- Create: `apps/web/src/components/FullReportView.tsx`
  - 전체 리포트 페이지형 뷰를 담당한다.
- Modify: `apps/web/src/test/dashboard.test.tsx`
  - 탭, 카드, 모달, 전체 리포트 전환까지 테스트한다.

## Task 1: 정보 구조를 테스트로 잠근다

**Files:**
- Modify: `apps/web/src/test/dashboard.test.tsx`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: 새 레이아웃 요구를 표현하는 failing test를 추가한다**

```tsx
// apps/web/src/test/dashboard.test.tsx
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import App from "../App";

describe("dashboard redesign", () => {
  it("renders league tabs and summary metadata before the match grid", () => {
    render(<App />);

    expect(screen.getByRole("tab", { name: "Premier League" })).toBeInTheDocument();
    expect(screen.getByText(/matches/i)).toBeInTheDocument();
    expect(screen.getByText(/need review/i)).toBeInTheDocument();
  });

  it("opens a match detail modal from the match card", () => {
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: /Chelsea vs Manchester City/i }));

    expect(screen.getByRole("dialog", { name: /Chelsea vs Manchester City/i })).toBeInTheDocument();
    expect(screen.getByText("Recommended Pick")).toBeInTheDocument();
    expect(screen.getByText("Open full report")).toBeInTheDocument();
  });

  it("opens a full report view from the detail modal", () => {
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: /Chelsea vs Manchester City/i }));
    fireEvent.click(screen.getByRole("button", { name: /Open full report/i }));

    expect(screen.getByRole("heading", { name: /Match report/i })).toBeInTheDocument();
    expect(screen.getByText(/Prediction summary/i)).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: 테스트를 실행해 실패를 확인한다**

Run: `npm --workspace apps/web run test -- --run src/test/dashboard.test.tsx`

Expected: `tab`, `dialog`, `Open full report` 관련 assertion FAIL.

- [ ] **Step 3: 기존 단순 heading 검증은 유지하되 새 구조를 기준으로 테스트를 정리한다**

```tsx
// 기존 "Football Prediction Dashboard" 존재 검증은 유지
// 기존 "Arsenal vs Chelsea", "48%", "Checkpoints" 같은 텍스트 나열 검증은 제거
```

- [ ] **Step 4: 테스트를 다시 실행해 실패가 구조 요구 때문인지 확인한다**

Run: `npm --workspace apps/web run test -- --run src/test/dashboard.test.tsx`

Expected: FAIL, but failure should now point to missing league tabs / modal / report view.

- [ ] **Step 5: 커밋한다**

```bash
git add apps/web/src/test/dashboard.test.tsx
git commit -m "대시보드 재설계 정보 구조를 테스트로 고정한다"
```

## Task 2: 타입과 상위 레이아웃 상태를 정리한다

**Files:**
- Modify: `apps/web/src/lib/api.ts`
- Modify: `apps/web/src/App.tsx`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: 카드/탭/리포트용 타입을 추가한다**

```ts
// apps/web/src/lib/api.ts
export interface LeagueSummary {
  id: string;
  label: string;
  matchCount: number;
  reviewCount: number;
}

export interface MatchCardRow extends MatchRow {
  leagueId: string;
  recommendedPick: string;
  confidence: number;
  needsReview: boolean;
  checkpointCount: number;
}

export interface MatchReport {
  matchId: string;
  title: string;
  status: string;
  prediction: PredictionSummary;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview;
}
```

- [ ] **Step 2: `App.tsx`에 샘플 리그/카드/리포트 상태를 추가한다**

```tsx
// apps/web/src/App.tsx
const leagues: LeagueSummary[] = [
  { id: "premier-league", label: "Premier League", matchCount: 12, reviewCount: 3 },
  { id: "ucl", label: "UCL", matchCount: 4, reviewCount: 1 },
  { id: "uel", label: "UEL", matchCount: 6, reviewCount: 1 },
  { id: "kor", label: "K League", matchCount: 5, reviewCount: 0 },
];

const matchCards: MatchCardRow[] = [
  {
    id: "match-001",
    leagueId: "premier-league",
    homeTeam: "Chelsea",
    awayTeam: "Manchester City",
    kickoffAt: "2026-04-27 19:00 UTC",
    status: "Final",
    recommendedPick: "HOME",
    confidence: 0.7,
    needsReview: true,
    checkpointCount: 4,
  },
  {
    id: "match-002",
    leagueId: "premier-league",
    homeTeam: "Liverpool",
    awayTeam: "Brentford",
    kickoffAt: "2026-04-27 21:00 UTC",
    status: "Scheduled",
    recommendedPick: "HOME",
    confidence: 0.58,
    needsReview: false,
    checkpointCount: 2,
  },
];

const [selectedLeagueId, setSelectedLeagueId] = useState("premier-league");
const [selectedMatchId, setSelectedMatchId] = useState<string | null>(null);
const [isModalOpen, setIsModalOpen] = useState(false);
const [reportMatchId, setReportMatchId] = useState<string | null>(null);
```

- [ ] **Step 3: `App.tsx`에서 카드 목록 필터링과 기본 선택 로직만 넣는다**

```tsx
const visibleMatches = matchCards.filter((match) => match.leagueId === selectedLeagueId);
const selectedMatch = visibleMatches.find((match) => match.id === selectedMatchId) ?? visibleMatches[0] ?? null;

function openMatch(matchId: string) {
  setSelectedMatchId(matchId);
  setIsModalOpen(true);
}

function openFullReport(matchId: string) {
  setReportMatchId(matchId);
  setIsModalOpen(false);
}
```

- [ ] **Step 4: 테스트를 실행해 여전히 UI 컴포넌트 부재로 실패하는지 확인한다**

Run: `npm --workspace apps/web run test -- --run src/test/dashboard.test.tsx`

Expected: FAIL, but not because of missing state variables or type errors.

- [ ] **Step 5: 커밋한다**

```bash
git add apps/web/src/lib/api.ts apps/web/src/App.tsx
git commit -m "대시보드 재설계용 상태와 타입을 준비한다"
```

## Task 3: 리그 탭과 카드 그리드를 만든다

**Files:**
- Create: `apps/web/src/components/LeagueTabs.tsx`
- Create: `apps/web/src/components/MatchCard.tsx`
- Modify: `apps/web/src/components/MatchTable.tsx`
- Modify: `apps/web/src/App.tsx`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: `LeagueTabs` failing expectations를 기준으로 컴포넌트를 만든다**

```tsx
// apps/web/src/components/LeagueTabs.tsx
import type { LeagueSummary } from "../lib/api";

interface LeagueTabsProps {
  leagues: LeagueSummary[];
  selectedLeagueId: string;
  onSelect: (leagueId: string) => void;
}

export default function LeagueTabs({
  leagues,
  selectedLeagueId,
  onSelect,
}: LeagueTabsProps) {
  const currentLeague = leagues.find((league) => league.id === selectedLeagueId);

  return (
    <section aria-label="league navigation">
      <div role="tablist" aria-label="Leagues">
        {leagues.map((league) => (
          <button
            key={league.id}
            role="tab"
            aria-selected={league.id === selectedLeagueId}
            onClick={() => onSelect(league.id)}
          >
            {league.label}
          </button>
        ))}
      </div>
      {currentLeague ? (
        <p>
          {currentLeague.matchCount} matches · {currentLeague.reviewCount} need review
        </p>
      ) : null}
    </section>
  );
}
```

- [ ] **Step 2: `MatchCard`를 만든다**

```tsx
// apps/web/src/components/MatchCard.tsx
import type { MatchCardRow } from "../lib/api";

interface MatchCardProps {
  match: MatchCardRow;
  onOpen: (matchId: string) => void;
}

export default function MatchCard({ match, onOpen }: MatchCardProps) {
  return (
    <button type="button" onClick={() => onOpen(match.id)} aria-label={`${match.homeTeam} vs ${match.awayTeam}`}>
      <article>
        <header>
          <div>
            <strong>{match.homeTeam} vs {match.awayTeam}</strong>
            <p>{match.kickoffAt}</p>
          </div>
          {match.needsReview ? <span>Needs Review</span> : null}
        </header>
        <div>
          <p>{match.status}</p>
          <p>Pick {match.recommendedPick}</p>
          <p>Confidence {match.confidence.toFixed(2)}</p>
        </div>
      </article>
    </button>
  );
}
```

- [ ] **Step 3: `MatchTable`을 카드 그리드 컨테이너로 바꾼다**

```tsx
// apps/web/src/components/MatchTable.tsx
import type { MatchCardRow } from "../lib/api";
import MatchCard from "./MatchCard";

interface MatchTableProps {
  matches: MatchCardRow[];
  onOpen: (matchId: string) => void;
}

export default function MatchTable({ matches, onOpen }: MatchTableProps) {
  return (
    <section aria-label="matches">
      {matches.length === 0 ? (
        <p>No matches available.</p>
      ) : (
        <div>
          {matches.map((match) => (
            <MatchCard key={match.id} match={match} onOpen={onOpen} />
          ))}
        </div>
      )}
    </section>
  );
}
```

- [ ] **Step 4: `App.tsx`에 리그 탭과 카드 그리드를 연결한다**

```tsx
// App render 일부
<LeagueTabs
  leagues={leagues}
  selectedLeagueId={selectedLeagueId}
  onSelect={setSelectedLeagueId}
/>
<MatchTable matches={visibleMatches} onOpen={openMatch} />
```

- [ ] **Step 5: 테스트를 실행해 탭/카드 관련 assertion이 통과하는지 확인한다**

Run: `npm --workspace apps/web run test -- --run src/test/dashboard.test.tsx`

Expected: 탭과 경기 카드 관련 테스트 PASS, 모달/리포트 관련 테스트 FAIL.

- [ ] **Step 6: 커밋한다**

```bash
git add apps/web/src/App.tsx apps/web/src/components/LeagueTabs.tsx apps/web/src/components/MatchCard.tsx apps/web/src/components/MatchTable.tsx apps/web/src/test/dashboard.test.tsx
git commit -m "리그 탭과 경기 카드 그리드를 도입한다"
```

## Task 4: 분석용 상세 모달을 구현한다

**Files:**
- Create: `apps/web/src/components/ProbabilityBars.tsx`
- Create: `apps/web/src/components/MatchDetailModal.tsx`
- Modify: `apps/web/src/components/PredictionCard.tsx`
- Modify: `apps/web/src/components/CheckpointTimeline.tsx`
- Modify: `apps/web/src/components/PostMatchReviewCard.tsx`
- Modify: `apps/web/src/App.tsx`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: `ProbabilityBars`를 만든다**

```tsx
// apps/web/src/components/ProbabilityBars.tsx
interface ProbabilityBarsProps {
  home: number;
  draw: number;
  away: number;
}

export default function ProbabilityBars({ home, draw, away }: ProbabilityBarsProps) {
  return (
    <div aria-label="probability bars">
      <div>Home {home}%</div>
      <div>Draw {draw}%</div>
      <div>Away {away}%</div>
    </div>
  );
}
```

- [ ] **Step 2: `PredictionCard`를 큰 추천 픽 + 작은 확률 바 구조로 바꾼다**

```tsx
// apps/web/src/components/PredictionCard.tsx
import type { PredictionSummary } from "../lib/api";
import ProbabilityBars from "./ProbabilityBars";

interface PredictionCardProps {
  prediction: PredictionSummary;
  recommendedPick: string;
  confidence: number;
}

export default function PredictionCard({
  prediction,
  recommendedPick,
  confidence,
}: PredictionCardProps) {
  return (
    <section aria-label="prediction summary">
      <p>Recommended Pick</p>
      <h2>{recommendedPick}</h2>
      <p>Confidence {confidence.toFixed(2)}</p>
      <ProbabilityBars
        home={prediction.homeWinProbability}
        draw={prediction.drawProbability}
        away={prediction.awayWinProbability}
      />
    </section>
  );
}
```

- [ ] **Step 3: `MatchDetailModal`을 만든다**

```tsx
// apps/web/src/components/MatchDetailModal.tsx
import type { MatchCardRow, PostMatchReview, PredictionSummary, TimelineCheckpoint } from "../lib/api";
import CheckpointTimeline from "./CheckpointTimeline";
import PostMatchReviewCard from "./PostMatchReviewCard";
import PredictionCard from "./PredictionCard";

interface MatchDetailModalProps {
  match: MatchCardRow | null;
  isOpen: boolean;
  prediction: PredictionSummary;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview;
  onClose: () => void;
  onOpenReport: (matchId: string) => void;
}

export default function MatchDetailModal({
  match,
  isOpen,
  prediction,
  checkpoints,
  review,
  onClose,
  onOpenReport,
}: MatchDetailModalProps) {
  if (!isOpen || !match) return null;

  return (
    <div role="dialog" aria-modal="true" aria-label={`${match.homeTeam} vs ${match.awayTeam}`}>
      <button type="button" onClick={onClose}>Close</button>
      <header>
        <h2>{match.homeTeam} vs {match.awayTeam}</h2>
        <p>{match.kickoffAt}</p>
        <p>{match.status}</p>
      </header>
      <PredictionCard
        prediction={prediction}
        recommendedPick={match.recommendedPick}
        confidence={match.confidence}
      />
      <PostMatchReviewCard review={review} />
      <CheckpointTimeline checkpoints={checkpoints} />
      <button type="button" onClick={() => onOpenReport(match.id)}>Open full report</button>
    </div>
  );
}
```

- [ ] **Step 4: `App.tsx`에 모달 상태를 연결한다**

```tsx
<MatchDetailModal
  match={selectedMatch}
  isOpen={isModalOpen}
  prediction={prediction}
  checkpoints={checkpoints}
  review={review}
  onClose={() => setIsModalOpen(false)}
  onOpenReport={openFullReport}
/>
```

- [ ] **Step 5: 테스트를 실행해 모달 관련 assertion이 통과하는지 확인한다**

Run: `npm --workspace apps/web run test -- --run src/test/dashboard.test.tsx`

Expected: 모달 open 테스트 PASS, full report 테스트 FAIL.

- [ ] **Step 6: 커밋한다**

```bash
git add apps/web/src/App.tsx apps/web/src/components/ProbabilityBars.tsx apps/web/src/components/MatchDetailModal.tsx apps/web/src/components/PredictionCard.tsx apps/web/src/components/CheckpointTimeline.tsx apps/web/src/components/PostMatchReviewCard.tsx apps/web/src/test/dashboard.test.tsx
git commit -m "경기 분석용 상세 모달을 추가한다"
```

## Task 5: 전체 리포트 페이지형 뷰를 추가한다

**Files:**
- Create: `apps/web/src/components/FullReportView.tsx`
- Modify: `apps/web/src/App.tsx`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: `FullReportView`를 만든다**

```tsx
// apps/web/src/components/FullReportView.tsx
import type { MatchCardRow, PostMatchReview, PredictionSummary, TimelineCheckpoint } from "../lib/api";
import CheckpointTimeline from "./CheckpointTimeline";
import PostMatchReviewCard from "./PostMatchReviewCard";
import PredictionCard from "./PredictionCard";

interface FullReportViewProps {
  match: MatchCardRow;
  prediction: PredictionSummary;
  checkpoints: TimelineCheckpoint[];
  review: PostMatchReview;
  onBack: () => void;
}

export default function FullReportView({
  match,
  prediction,
  checkpoints,
  review,
  onBack,
}: FullReportViewProps) {
  return (
    <section aria-label="match report">
      <button type="button" onClick={onBack}>Back to dashboard</button>
      <h1>Match report</h1>
      <PredictionCard
        prediction={prediction}
        recommendedPick={match.recommendedPick}
        confidence={match.confidence}
      />
      <h2>Prediction summary</h2>
      <CheckpointTimeline checkpoints={checkpoints} />
      <PostMatchReviewCard review={review} />
    </section>
  );
}
```

- [ ] **Step 2: `App.tsx`에서 리포트 뷰 전환을 추가한다**

```tsx
if (reportMatchId && selectedMatch) {
  return (
    <main>
      <FullReportView
        match={selectedMatch}
        prediction={prediction}
        checkpoints={checkpoints}
        review={review}
        onBack={() => setReportMatchId(null)}
      />
    </main>
  );
}
```

- [ ] **Step 3: 테스트를 실행해 전체 리포트 관련 assertion이 통과하는지 확인한다**

Run: `npm --workspace apps/web run test -- --run src/test/dashboard.test.tsx`

Expected: dashboard redesign 테스트 전체 PASS.

- [ ] **Step 4: 커밋한다**

```bash
git add apps/web/src/App.tsx apps/web/src/components/FullReportView.tsx apps/web/src/test/dashboard.test.tsx
git commit -m "경기 전체 리포트 뷰를 추가한다"
```

## Task 6: 운영형 마감 다듬기

**Files:**
- Modify: `apps/web/src/App.tsx`
- Modify: `apps/web/src/components/LeagueTabs.tsx`
- Modify: `apps/web/src/components/MatchCard.tsx`
- Modify: `apps/web/src/components/MatchDetailModal.tsx`
- Modify: `apps/web/src/components/FullReportView.tsx`
- Test: `apps/web/src/test/dashboard.test.tsx`

- [ ] **Step 1: 불필요한 문구와 과한 시각 장치를 줄인다**

```tsx
// 점검 항목
// - 설명 문장을 한 줄로 줄일 것
// - 카드 안의 중복 라벨 제거
// - 모든 상태를 색으로 표현하지 말 것
// - Needs Review 외에는 조용한 상태 표현 유지
```

- [ ] **Step 2: 모달과 리포트 뷰의 heading hierarchy를 정리한다**

```tsx
// 확인 항목
// - 모달: 경기명 > prediction summary > review > checkpoints
// - 리포트: Match report > Prediction summary > Checkpoint changes > Review analysis
```

- [ ] **Step 3: 전체 테스트를 실행한다**

Run: `npm test`
Expected: PASS

Run: `npm --workspace apps/api run test`
Expected: PASS

Run: `npm --workspace apps/web run test`
Expected: PASS

Run: `python3 -m pytest`
Expected: PASS

- [ ] **Step 4: 변경 검토 명령을 실행한다**

Run: `git status --short`
Expected: 수정 파일만 표시

Run: `git diff --stat`
Expected: 웹 앱 파일 중심 diff 확인

Run: `git diff --check`
Expected: no output

- [ ] **Step 5: 커밋한다**

```bash
git add apps/web/src/App.tsx apps/web/src/components/LeagueTabs.tsx apps/web/src/components/MatchCard.tsx apps/web/src/components/MatchDetailModal.tsx apps/web/src/components/FullReportView.tsx apps/web/src/test/dashboard.test.tsx
git commit -m "운영형 대시보드 레이아웃을 마감한다"
```

## Self-Review

- 스펙 커버리지:
  - 리그 탭, 경기 수/리뷰 수, 2열 카드, 좌측 액센트 바, 분석 모달, 전체 리포트 페이지를 모두 포함했다.
  - 상태 강조, 반응형 기본 원칙, 카드/모달/페이지 책임 분리도 작업 단위에 반영했다.
- placeholder scan:
  - `TODO`, `TBD`, “적절히 처리” 같은 문구는 넣지 않았다.
  - 각 작업마다 실제 파일 경로, 테스트, 명령을 적었다.
- 타입 일관성:
  - `LeagueSummary`, `MatchCardRow`, `MatchDetailModal`, `FullReportView` 이름을 계획 전반에서 일관되게 사용했다.

Plan complete and saved to `docs/superpowers/plans/2026-04-19-dashboard-redesign-implementation-plan.md`.

Two execution options:

1. Subagent-Driven (recommended) - fresh subagent per task, review between tasks
2. Inline Execution - execute tasks in this session in sequence
