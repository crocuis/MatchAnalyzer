// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { StrictMode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "../App";
import DailyPicksTeaser from "../components/DailyPicksTeaser";
import FullReportView from "../components/FullReportView";
import MatchCard from "../components/MatchCard";
import MatchDetailModal from "../components/MatchDetailModal";
import i18n from "../i18n/config";
import { fetchDailyPicks } from "../lib/api";
import type { MatchCardRow, PredictionSummary } from "../lib/api";

afterEach(() => {
  cleanup();
  vi.unstubAllEnvs();
});

beforeEach(async () => {
  await i18n.changeLanguage("en");
  vi.stubEnv("VITE_API_BASE_URL", "");

  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url.startsWith("/api/matches")) {
        const parsedUrl = new URL(url, "http://localhost");
        const leagueId = parsedUrl.searchParams.get("leagueId") ?? "premier-league";
        const cursor = Number.parseInt(parsedUrl.searchParams.get("cursor") ?? "0", 10) || 0;
        const limit = Number.parseInt(parsedUrl.searchParams.get("limit") ?? "4", 10) || 4;
        const allItems = [
          {
            id: "match-001",
            leagueId: "premier-league",
            leagueLabel: "Premier League",
            leagueEmblemUrl: "https://crests.football-data.org/PL.png",
            homeTeam: "Chelsea",
            homeTeamLogoUrl: "https://crests.football-data.org/61.png",
            awayTeam: "Manchester City",
            awayTeamLogoUrl: "https://crests.football-data.org/65.png",
            kickoffAt: "2026-04-27 19:00 UTC",
            status: "Needs Review",
            finalResult: "AWAY",
            homeScore: 1,
            awayScore: 2,
            recommendedPick: "HOME",
            confidence: 0.7,
            explanationPayload: {
              sourceAgreementRatio: 1,
              maxAbsDivergence: 0.05,
              calibratedConfidence: 0.7,
              featureContext: {
                eloDelta: 0.42,
                xgProxyDelta: 0.31,
                fixtureCongestionDelta: 1,
                lineupStrengthDelta: 0.61,
                homeLineupScore: 1.82,
                awayLineupScore: 1.21,
                lineupSourceSummary: "espn_lineups+recent_starters+pl_missing_players",
              },
            },
            needsReview: true,
          },
          {
            id: "match-002",
            leagueId: "premier-league",
            leagueLabel: "Premier League",
            leagueEmblemUrl: "https://crests.football-data.org/PL.png",
            homeTeam: "Liverpool",
            awayTeam: "Brentford",
            kickoffAt: "2026-04-27 21:00 UTC",
            status: "Prediction Ready",
            recommendedPick: null,
            confidence: null,
            mainRecommendation: {
              pick: "HOME",
              confidence: 0.58,
              recommended: false,
              noBetReason: "low_confidence",
            },
            valueRecommendation: {
              pick: "AWAY",
              recommended: true,
              edge: 0.1,
              expectedValue: 0.3125,
              marketPrice: 0.24,
              modelProbability: 0.42,
              marketProbability: 0.32,
              marketSource: "prediction_market",
            },
            variantMarkets: [
              {
                marketFamily: "spreads",
                sourceName: "polymarket_spreads",
                lineValue: -0.5,
                selectionALabel: "Home -0.5",
                selectionAPrice: 0.54,
                selectionBLabel: "Away +0.5",
                selectionBPrice: 0.46,
                marketSlug: "spread-slug",
              },
              {
                marketFamily: "totals",
                sourceName: "polymarket_totals",
                lineValue: 2.5,
                selectionALabel: "Over 2.5",
                selectionAPrice: 0.57,
                selectionBLabel: "Under 2.5",
                selectionBPrice: 0.43,
                marketSlug: "total-slug",
              },
            ],
            needsReview: false,
          },
          {
            id: "match-004",
            leagueId: "premier-league",
            leagueLabel: "Premier League",
            leagueEmblemUrl: "https://crests.football-data.org/PL.png",
            homeTeam: "Arsenal",
            awayTeam: "Fulham",
            kickoffAt: "2026-04-20 19:00 UTC",
            status: "Scheduled",
            recommendedPick: null,
            confidence: null,
            needsReview: false,
          },
          {
            id: "match-003",
            leagueId: "champions-league",
            leagueLabel: "UEFA Champions League",
            leagueEmblemUrl: "https://crests.football-data.org/CL.png",
            homeTeam: "Inter",
            awayTeam: "Bayern Munich",
            kickoffAt: "2026-04-28 19:00 UTC",
            status: "Review Ready",
            finalResult: "DRAW",
            homeScore: 1,
            awayScore: 1,
            recommendedPick: "DRAW",
            confidence: 0.41,
            explanationPayload: {
              sourceAgreementRatio: 0.67,
              featureContext: {
                eloDelta: -0.33,
                xgProxyDelta: -0.26,
                fixtureCongestionDelta: -1,
                lineupStrengthDelta: -0.58,
                homeLineupScore: 1.14,
                awayLineupScore: 1.72,
                lineupSourceSummary: "espn_lineups+recent_starters",
              },
            },
            needsReview: true,
          },
        ];
        const leagues = [
          {
            id: "premier-league",
            label: "Premier League",
            emblemUrl: "https://crests.football-data.org/PL.png",
            matchCount: 3,
            reviewCount: 1,
          },
          {
            id: "champions-league",
            label: "UEFA Champions League",
            emblemUrl: "https://crests.football-data.org/CL.png",
            matchCount: 1,
            reviewCount: 1,
          },
        ];
        const filtered = allItems.filter((item) => item.leagueId === leagueId);
        return {
          ok: true,
          json: async () => ({
            items: filtered.slice(cursor, cursor + limit),
            leagues,
            predictionSummary:
              leagueId === "champions-league"
                ? {
                    predictedCount: 1,
                    evaluatedCount: 1,
                    correctCount: 1,
                    incorrectCount: 0,
                    successRate: 1,
                  }
                : {
                    predictedCount: 2,
                    evaluatedCount: 1,
                    correctCount: 0,
                    incorrectCount: 1,
                    successRate: 0,
                  },
            selectedLeagueId: leagueId,
            nextCursor: cursor + limit < filtered.length ? String(cursor + limit) : null,
            totalMatches: filtered.length,
          }),
        };
      }

      if (url.endsWith("/api/predictions/source-evaluation/latest")) {
        return {
          ok: true,
          json: async () => ({
            report: {
              generatedAt: "2026-04-28T08:30:00Z",
              snapshotsEvaluated: 8,
              rowsEvaluated: 30,
              overall: {
                bookmaker: {
                  count: 8,
                  hitRate: 0.5,
                  avgBrierScore: 0.2211,
                  avgLogLoss: 0.6342,
                },
                base_model: {
                  count: 8,
                  hitRate: 0.625,
                  avgBrierScore: 0.2012,
                  avgLogLoss: 0.5844,
                },
                current_fused: {
                  count: 8,
                  hitRate: 0.75,
                  avgBrierScore: 0.1812,
                  avgLogLoss: 0.5511,
                },
                prediction_market: {
                  count: 6,
                  hitRate: 0.6667,
                  avgBrierScore: 0.1933,
                  avgLogLoss: 0.5699,
                },
              },
              byCheckpoint: {
                LINEUP_CONFIRMED: {
                  current_fused: {
                    count: 3,
                    hitRate: 0.6667,
                    avgBrierScore: 0.19,
                    avgLogLoss: 0.58,
                  },
                  prediction_market: {
                    count: 3,
                    hitRate: 0.3333,
                    avgBrierScore: 0.24,
                    avgLogLoss: 0.63,
                  },
                },
              },
              byMarketSegment: {
                with_prediction_market: {
                  current_fused: {
                    count: 6,
                    hitRate: 0.8333,
                    avgBrierScore: 0.17,
                    avgLogLoss: 0.52,
                  },
                },
              },
            },
          }),
        };
      }

      if (url.endsWith("/api/predictions/source-evaluation/history")) {
        return {
          ok: true,
          json: async () => ({
            latest: {
              generatedAt: "2026-04-28T08:30:00Z",
              snapshotsEvaluated: 8,
              rowsEvaluated: 30,
              overall: {
                bookmaker: {
                  count: 8,
                  hitRate: 0.5,
                  avgBrierScore: 0.2211,
                  avgLogLoss: 0.6342,
                },
                current_fused: {
                  count: 8,
                  hitRate: 0.75,
                  avgBrierScore: 0.1812,
                  avgLogLoss: 0.5511,
                },
              },
              byCheckpoint: {
                LINEUP_CONFIRMED: {
                  current_fused: {
                    count: 3,
                    hitRate: 0.6667,
                    avgBrierScore: 0.19,
                    avgLogLoss: 0.58,
                  },
                  prediction_market: {
                    count: 3,
                    hitRate: 0.3333,
                    avgBrierScore: 0.24,
                    avgLogLoss: 0.63,
                  },
                },
              },
              byCompetition: {},
              byMarketSegment: {
                with_prediction_market: {
                  current_fused: {
                    count: 6,
                    hitRate: 0.8333,
                    avgBrierScore: 0.17,
                    avgLogLoss: 0.52,
                  },
                },
              },
            },
            previous: {
              generatedAt: "2026-04-21T08:30:00Z",
              snapshotsEvaluated: 6,
              rowsEvaluated: 22,
              overall: {
                bookmaker: {
                  count: 6,
                  hitRate: 0.5,
                  avgBrierScore: 0.2362,
                  avgLogLoss: 0.6481,
                },
                current_fused: {
                  count: 6,
                  hitRate: 0.5,
                  avgBrierScore: 0.2055,
                  avgLogLoss: 0.6032,
                },
              },
              byCheckpoint: {},
              byCompetition: {},
              byMarketSegment: {},
            },
            history: [
              {
                id: "eval-latest",
                createdAt: "2026-04-28T08:30:00Z",
                report: {
                  generatedAt: "2026-04-28T08:30:00Z",
                  snapshotsEvaluated: 8,
                  rowsEvaluated: 30,
                  overall: {
                    current_fused: {
                      count: 8,
                      hitRate: 0.75,
                      avgBrierScore: 0.1812,
                      avgLogLoss: 0.5511,
                    },
                  },
                  byCheckpoint: {
                    LINEUP_CONFIRMED: {
                      current_fused: {
                        count: 3,
                        hitRate: 0.6667,
                        avgBrierScore: 0.19,
                        avgLogLoss: 0.58,
                      },
                    },
                  },
                  byCompetition: {},
                  byMarketSegment: {
                    with_prediction_market: {
                      current_fused: {
                        count: 6,
                        hitRate: 0.8333,
                        avgBrierScore: 0.17,
                        avgLogLoss: 0.52,
                      },
                    },
                  },
                },
              },
              {
                id: "eval-prev",
                createdAt: "2026-04-21T08:30:00Z",
                report: {
                  generatedAt: "2026-04-21T08:30:00Z",
                  snapshotsEvaluated: 6,
                  rowsEvaluated: 22,
                  overall: {
                    current_fused: {
                      count: 6,
                      hitRate: 0.5,
                      avgBrierScore: 0.2055,
                      avgLogLoss: 0.6032,
                    },
                  },
                  byCheckpoint: {},
                  byCompetition: {},
                  byMarketSegment: {},
                },
              },
            ],
            shadow: {
              status: "running",
              baseline: "current_fused",
              candidate: "shadow_candidate_v2",
              summary: "Shadow evaluation in progress",
              trafficPercent: null,
            },
            rollout: {
              status: "ramped",
              baseline: null,
              candidate: null,
              summary: "Rollout increased to 25%",
              trafficPercent: 25,
            },
          }),
        };
      }

      if (url.endsWith("/api/predictions/model-registry/latest")) {
        return {
          ok: true,
          json: async () => ({
            report: {
              id: "model_v1",
              modelFamily: "baseline",
              trainingWindow: "2024-2026",
              featureVersion: "features_v1",
              calibrationVersion: "isotonic_v1",
              createdAt: "2026-04-28T08:40:00Z",
              selectionMetadata: {
                byCheckpoint: {
                  LINEUP_CONFIRMED: {
                    selectedCandidate: "logistic_regression",
                    selectionMetric: "neg_log_loss",
                    selectionRan: true,
                    candidateScores: {
                      hist_gradient_boosting: 0.59,
                      logistic_regression: 0.83,
                    },
                    fallbackSource: null,
                  },
                },
              },
              trainingMetadata: {
                selectionCount: 1,
              },
            },
          }),
        };
      }

      if (url.endsWith("/api/predictions/fusion-policy/latest")) {
        return {
          ok: true,
          json: async () => ({
            report: {
              id: "latest",
              sourceReportId: "latest",
              createdAt: "2026-04-28T08:45:00Z",
              policyId: "latest",
              policyVersion: 1,
              selectionOrder: [
                "by_checkpoint_market_segment",
                "by_checkpoint",
                "by_market_segment",
                "overall",
              ],
              weights: {
                overall: {
                  base_model: 0.34,
                  bookmaker: 0.33,
                  prediction_market: 0.33,
                },
              },
            },
          }),
        };
      }

      if (url.endsWith("/api/predictions/fusion-policy/history")) {
        return {
          ok: true,
          json: async () => ({
            latest: {
              id: "latest",
              sourceReportId: "latest",
              createdAt: "2026-04-28T08:45:00Z",
              policyId: "latest",
              policyVersion: 3,
              selectionOrder: [
                "by_checkpoint_market_segment",
                "by_checkpoint",
                "by_market_segment",
                "overall",
              ],
              weights: {
                overall: {
                  base_model: 0.3,
                  bookmaker: 0.25,
                  prediction_market: 0.45,
                },
              },
            },
            previous: {
              id: "previous",
              sourceReportId: "previous",
              createdAt: "2026-04-21T08:45:00Z",
              policyId: "previous",
              policyVersion: 2,
              selectionOrder: ["by_checkpoint", "overall"],
              weights: {
                overall: {
                  base_model: 0.4,
                  bookmaker: 0.3,
                  prediction_market: 0.3,
                },
              },
            },
            history: [
              {
                id: "latest",
                createdAt: "2026-04-28T08:45:00Z",
                report: {
                  id: "latest",
                  sourceReportId: "latest",
                  createdAt: "2026-04-28T08:45:00Z",
                  policyId: "latest",
                  policyVersion: 3,
                  selectionOrder: [
                    "by_checkpoint_market_segment",
                    "by_checkpoint",
                    "by_market_segment",
                    "overall",
                  ],
                  weights: {
                    overall: {
                      base_model: 0.3,
                      bookmaker: 0.25,
                      prediction_market: 0.45,
                    },
                  },
                },
              },
              {
                id: "previous",
                createdAt: "2026-04-21T08:45:00Z",
                report: {
                  id: "previous",
                  sourceReportId: "previous",
                  createdAt: "2026-04-21T08:45:00Z",
                  policyId: "previous",
                  policyVersion: 2,
                  selectionOrder: ["by_checkpoint", "overall"],
                  weights: {
                    overall: {
                      base_model: 0.4,
                      bookmaker: 0.3,
                      prediction_market: 0.3,
                    },
                  },
                },
              },
            ],
            shadow: {
              status: "candidate",
              baseline: "policy_v2",
              candidate: "policy_v3",
              summary: "Candidate weights outperform baseline",
              trafficPercent: null,
            },
            rollout: {
              status: "guarded",
              baseline: "policy_v2",
              candidate: "policy_v3",
              summary: "Guarded rollout at 30%",
              trafficPercent: 30,
            },
          }),
        };
      }

      if (url.startsWith("/api/daily-picks")) {
        return {
          ok: true,
          json: async () => {
            const items = [
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
                id: "pick-3",
                matchId: "match-003",
                predictionId: "prediction-3",
                leagueId: "champions-league",
                leagueLabel: "UEFA Champions League",
                homeTeam: "Inter",
                homeTeamLogoUrl: "https://crests.football-data.org/108.png",
                awayTeam: "Bayern Munich",
                awayTeamLogoUrl: "https://crests.football-data.org/5.png",
                kickoffAt: "2026-04-28 19:00 UTC",
                marketFamily: "moneyline",
                selectionLabel: "DRAW",
                confidence: 0.71,
                edge: 0.1,
                expectedValue: 0.21,
                marketPrice: 0.53,
                modelProbability: 0.64,
                marketProbability: 0.53,
                sourceAgreementRatio: 0.7,
                status: "recommended",
                noBetReason: null,
                reasonLabels: ["mainRecommendation"],
              },
            ];
            const heldItems = [
              {
                id: "held-spread-1",
                matchId: "match-002",
                predictionId: "prediction-2",
                leagueId: "premier-league",
                leagueLabel: "Premier League",
                homeTeam: "Liverpool",
                awayTeam: "Brentford",
                kickoffAt: "2026-04-27 21:00 UTC",
                marketFamily: "spreads",
                selectionLabel: "Home -0.5",
                confidence: null,
                edge: null,
                expectedValue: null,
                marketPrice: 0.58,
                modelProbability: null,
                marketProbability: 0.58,
                sourceAgreementRatio: 0.75,
                status: "held",
                noBetReason: "variant_market_price_only",
                reasonLabels: ["spreads", "heldByRecommendationGate"],
              },
              {
                id: "held-total-1",
                matchId: "match-003",
                predictionId: "prediction-3",
                leagueId: "champions-league",
                leagueLabel: "UEFA Champions League",
                homeTeam: "Inter",
                homeTeamLogoUrl: "https://crests.football-data.org/108.png",
                awayTeam: "Bayern Munich",
                awayTeamLogoUrl: "https://crests.football-data.org/5.png",
                kickoffAt: "2026-04-28 19:00 UTC",
                marketFamily: "totals",
                selectionLabel: "Under 2.5",
                confidence: null,
                edge: null,
                expectedValue: null,
                marketPrice: 0.53,
                modelProbability: null,
                marketProbability: 0.53,
                sourceAgreementRatio: 0.7,
                status: "held",
                noBetReason: "variant_market_price_only",
                reasonLabels: ["totals", "heldByRecommendationGate"],
              },
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
            ];
            return {
              generatedAt: "2026-04-24T08:00:00Z",
              date: "2026-04-24",
              target: { minDailyRecommendations: 5, maxDailyRecommendations: 10, hitRate: 0.7, roi: 0.2 },
              validation: {
                hitRate: 0.75,
                sampleCount: 76,
                wilsonLowerBound: 0.6422,
                confidenceReliability: "validated",
                modelScope: "daily_pick_prequential",
              },
              coverage: { moneyline: 3, spreads: 1, totals: 1, held: 3 },
              items,
              heldItems,
            };
          },
        };
      }

      if (url.endsWith("/api/predictions/match-001")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-001",
            prediction: {
              matchId: "match-001",
              checkpointLabel: "LINEUP_CONFIRMED",
              homeWinProbability: 48,
              drawProbability: 27,
              awayWinProbability: 25,
              recommendedPick: "HOME",
              confidence: 0.7,
              valueRecommendation: {
                pick: "HOME",
                recommended: true,
                edge: 0.12,
                expectedValue: 0.4,
                marketPrice: 0.3,
                modelProbability: 0.42,
                marketProbability: 0.3,
                marketSource: "prediction_market",
              },
              variantMarkets: [
                {
                  marketFamily: "spreads",
                  sourceName: "polymarket_spreads",
                  lineValue: -0.5,
                  selectionALabel: "Home -0.5",
                  selectionAPrice: 0.54,
                  selectionBLabel: "Away +0.5",
                  selectionBPrice: 0.46,
                  marketSlug: "spread-slug",
                },
                {
                  marketFamily: "totals",
                  sourceName: "polymarket_totals",
                  lineValue: 2.5,
                  selectionALabel: "Over 2.5",
                  selectionAPrice: 0.43,
                  selectionBLabel: "Under 2.5",
                  selectionBPrice: 0.57,
                  marketSlug: "total-slug",
                },
              ],
              explanationPayload: {
                rawConfidence: 0.76,
                calibratedConfidence: 0.7,
                baseModelSource: "trained_baseline",
                baseModelProbs: {
                  home: 0.52,
                  draw: 0.24,
                  away: 0.24,
                },
                predictionMarketAvailable: true,
                sourcesAgree: true,
                sourceAgreementRatio: 1,
                maxAbsDivergence: 0.05,
                confidenceCalibration: {
                  "0.7-0.8": {
                    count: 6,
                    hitRate: 0.67,
                  },
                },
                featureAttribution: [
                  {
                    featureKey: "elo_delta",
                    signalKey: "strengthHome",
                    direction: "home",
                    magnitude: 0.42,
                  },
                  {
                    featureKey: "xg_proxy_delta",
                    signalKey: "xgHome",
                    direction: "home",
                    magnitude: 0.31,
                  },
                ],
                sourceMetadata: {
                  marketSegment: "with_prediction_market",
                  fusionWeights: {
                    bookmaker: 0.2,
                    predictionMarket: 0.55,
                    baseModel: 0.25,
                  },
                  fusionPolicy: {
                    policy_id: "latest",
                    matched_on: "by_checkpoint_market_segment",
                    policy_source: "prediction_fusion_policies",
                  },
                },
                featureContext: {
                  eloDelta: 0.42,
                  xgProxyDelta: 0.31,
                  fixtureCongestionDelta: 1,
                  lineupStrengthDelta: 0.61,
                  homeLineupScore: 1.82,
                  awayLineupScore: 1.21,
                  lineupSourceSummary: "espn_lineups+recent_starters+pl_missing_players",
                },
              },
            },
            checkpoints: [
              {
                id: "checkpoint-001",
                label: "T-24H",
                recordedAt: "2026-04-26 19:00 UTC",
                note: "complete snapshot · Pick HOME",
              },
            ],
          }),
        };
      }

      if (url.endsWith("/api/reviews/match-001")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-001",
            review: {
              matchId: "match-001",
              outcome: "Large directional miss",
              actualOutcome: "AWAY",
              summary:
                "Model favored HOME with high confidence, but the result flipped to AWAY.",
              causeTags: ["major_directional_miss"],
              taxonomy: {
                missFamily: "directional_miss",
                severity: "high",
                consensusLevel: "low",
                marketSignal: "market_outperformed_model",
              },
              attributionSummary: {
                primarySignal: "strengthHome",
                secondarySignal: "xgHome",
              },
              marketComparison: {
                comparison_available: true,
                market_outperformed_model: true,
              },
            },
          }),
        };
      }

      if (url.endsWith("/api/reviews/aggregation/latest")) {
        return {
          ok: true,
          json: async () => ({
            report: {
              totalReviews: 12,
              byMissFamily: {
                directional_miss: 7,
                draw_blind_spot: 3,
              },
              bySeverity: {
                high: 4,
                medium: 6,
                low: 2,
              },
              byPrimarySignal: {
                strengthHome: 5,
                xgHome: 4,
              },
              topMissFamily: "directional_miss",
              topPrimarySignal: "strengthHome",
              createdAt: "2026-04-28T08:50:00Z",
            },
          }),
        };
      }

      if (url.endsWith("/api/rollouts/promotion/latest")) {
        return {
          ok: true,
          json: async () => ({
            report: {
              status: "approved",
              recommendedAction: "promote_rollout",
              reasons: ["all_gates_passed"],
              gates: {
                sourceEvaluation: { status: "pass", hitRateDelta: 0.08 },
                reviewAggregation: { status: "pass", totalReviewsDelta: -1 },
                fusionPolicy: { status: "pass", maxWeightShift: 0.05 },
              },
              sourceReportId: "latest",
              fusionPolicyId: "latest",
              reviewAggregationId: "latest",
              createdAt: "2026-04-28T09:00:00Z",
            },
          }),
        };
      }

      if (url.endsWith("/api/reviews/aggregation/history")) {
        return {
          ok: true,
          json: async () => ({
            latest: {
              totalReviews: 12,
              byMissFamily: {
                directional_miss: 7,
                draw_blind_spot: 3,
              },
              bySeverity: {
                high: 4,
                medium: 6,
                low: 2,
              },
              byPrimarySignal: {
                strengthHome: 5,
                xgHome: 4,
              },
              topMissFamily: "directional_miss",
              topPrimarySignal: "strengthHome",
              createdAt: "2026-04-28T08:50:00Z",
            },
            previous: {
              totalReviews: 9,
              byMissFamily: {
                directional_miss: 4,
                draw_blind_spot: 2,
              },
              bySeverity: {
                high: 2,
                medium: 5,
                low: 2,
              },
              byPrimarySignal: {
                strengthHome: 3,
                xgHome: 3,
              },
              topMissFamily: "directional_miss",
              topPrimarySignal: "xgHome",
              createdAt: "2026-04-21T08:50:00Z",
            },
            history: [
              {
                id: "review-latest",
                createdAt: "2026-04-28T08:50:00Z",
                report: {
                  totalReviews: 12,
                  byMissFamily: {
                    directional_miss: 7,
                    draw_blind_spot: 3,
                  },
                  bySeverity: {
                    high: 4,
                    medium: 6,
                    low: 2,
                  },
                  byPrimarySignal: {
                    strengthHome: 5,
                    xgHome: 4,
                  },
                  topMissFamily: "directional_miss",
                  topPrimarySignal: "strengthHome",
                  createdAt: "2026-04-28T08:50:00Z",
                },
              },
              {
                id: "review-prev",
                createdAt: "2026-04-21T08:50:00Z",
                report: {
                  totalReviews: 9,
                  byMissFamily: {
                    directional_miss: 4,
                    draw_blind_spot: 2,
                  },
                  bySeverity: {
                    high: 2,
                    medium: 5,
                    low: 2,
                  },
                  byPrimarySignal: {
                    strengthHome: 3,
                    xgHome: 3,
                  },
                  topMissFamily: "directional_miss",
                  topPrimarySignal: "xgHome",
                  createdAt: "2026-04-21T08:50:00Z",
                },
              },
            ],
            shadow: {
              status: "reviewing",
              baseline: "manual_taxonomy_v1",
              candidate: "manual_taxonomy_v2",
              summary: "Shadow taxonomy review is collecting misses",
              trafficPercent: null,
            },
            rollout: {
              status: "ramped",
              baseline: "manual_taxonomy_v1",
              candidate: "manual_taxonomy_v2",
              summary: "Review aggregation rollout at 50%",
              trafficPercent: 50,
            },
          }),
        };
      }

      if (url.endsWith("/api/predictions/match-002")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-002",
            prediction: {
              matchId: "match-002",
              checkpointLabel: "T-12H",
              homeWinProbability: 58,
              drawProbability: 24,
              awayWinProbability: 18,
              recommendedPick: null,
              confidence: null,
              mainRecommendation: {
                pick: "HOME",
                confidence: 0.58,
                recommended: false,
                noBetReason: "low_confidence",
              },
              valueRecommendation: {
                pick: "AWAY",
                recommended: true,
                edge: 0.1,
                expectedValue: 0.3125,
                marketPrice: 0.24,
                modelProbability: 0.42,
                marketProbability: 0.32,
                marketSource: "prediction_market",
              },
              variantMarkets: [
                {
                  marketFamily: "spreads",
                  sourceName: "polymarket_spreads",
                  lineValue: -0.5,
                  selectionALabel: "Home -0.5",
                  selectionAPrice: 0.54,
                  selectionBLabel: "Away +0.5",
                  selectionBPrice: 0.46,
                  marketSlug: "spread-slug",
                },
                {
                  marketFamily: "totals",
                  sourceName: "polymarket_totals",
                  lineValue: 2.5,
                  selectionALabel: "Over 2.5",
                  selectionAPrice: 0.57,
                  selectionBLabel: "Under 2.5",
                  selectionBPrice: 0.43,
                  marketSlug: "total-slug",
                },
              ],
            },
            checkpoints: [],
          }),
        };
      }

      if (url.endsWith("/api/reviews/match-002")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-002",
            review: null,
          }),
        };
      }

      if (url.endsWith("/api/predictions/match-003")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-003",
            prediction: {
              matchId: "match-003",
              checkpointLabel: "T-18H",
              homeWinProbability: 34,
              drawProbability: 33,
              awayWinProbability: 33,
              recommendedPick: "DRAW",
              confidence: 0.41,
              explanationPayload: {
                rawConfidence: 0.49,
                calibratedConfidence: 0.41,
                baseModelSource: "centroid_fallback",
                sourceAgreementRatio: 0.67,
                maxAbsDivergence: 0.12,
                featureContext: {
                  eloDelta: -0.33,
                  xgProxyDelta: -0.26,
                  fixtureCongestionDelta: -1,
                  lineupStrengthDelta: -0.58,
                  homeLineupScore: 1.14,
                  awayLineupScore: 1.72,
                  lineupSourceSummary: "espn_lineups+recent_starters",
                },
              },
            },
            checkpoints: [],
          }),
        };
      }

      if (url.endsWith("/api/reviews/match-003")) {
        return {
          ok: true,
          json: async () => ({
            matchId: "match-003",
            review: {
              matchId: "match-003",
              outcome: "Review ready",
              summary: "Awaiting operator analysis.",
            },
          }),
        };
      }

      return {
        ok: true,
        json: async () => ({ matchId: "unknown", prediction: null, checkpoints: [], review: null }),
      };
    }),
  );
});

function hasTextContent(text: string) {
  return (_content: string, node: Element | null) =>
    node?.textContent?.replace(/\s+/g, " ").trim() === text;
}

it("fetches daily picks with filters", async () => {
  const fetchMock = vi.fn(async () => ({
    ok: true,
    json: async () => ({
      generatedAt: "2026-04-24T08:00:00Z",
      date: "2026-04-24",
      target: {
        minDailyRecommendations: 5,
        maxDailyRecommendations: 10,
        hitRate: 0.7,
        roi: 0.2,
      },
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

describe("dashboard redesign", () => {
  it("preserves the prediction workspace heading", () => {
    render(<App />);

    expect(screen.getByText("Match Analysis Hub")).toBeInTheDocument();
  });

  it("shows daily picks header and board entry points", async () => {
    render(<App />);

    const fetchMock = vi.mocked(fetch);
    const dailyPicksCalls = () =>
      fetchMock.mock.calls
        .map(([url]) => url)
        .filter((url): url is string => typeof url === "string" && url.startsWith("/api/daily-picks"));
    expect(await screen.findByRole("heading", { name: /^daily picks$/i })).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: /^view$/i })).toBeInTheDocument();
    expect(await screen.findByText("2 picks")).toBeInTheDocument();
    expect(await screen.findByText("Cumulative hit rate")).toBeInTheDocument();
    expect(await screen.findByText("75.0%")).toBeInTheDocument();
    expect(dailyPicksCalls()).toContainEqual(expect.stringMatching(/^\/api\/daily-picks\?date=\d{4}-\d{2}-\d{2}&locale=en$/));
  });

  it("surfaces held-only daily pick candidates in the teaser", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.startsWith("/api/daily-picks")) {
          return {
            ok: true,
            json: async () => ({
              generatedAt: "2026-04-29T08:00:00Z",
              date: "2026-04-29",
              target: { minDailyRecommendations: 5, maxDailyRecommendations: 10, hitRate: 0.7, roi: 0.2 },
              validation: {
                hitRate: 0.5652,
                sampleCount: 46,
                wilsonLowerBound: 0.4225,
                confidenceReliability: "settled_daily_picks",
                modelScope: "daily_pick_settled",
              },
              coverage: { moneyline: 1, spreads: 0, totals: 1, held: 2 },
              items: [],
              heldItems: [],
            }),
          };
        }
        return { ok: false, json: async () => ({}) };
      }),
    );

    render(<DailyPicksTeaser onOpen={vi.fn()} />);

    expect(await screen.findByText("Candidates")).toBeInTheDocument();
    expect(await screen.findByText("2 picks")).toBeInTheDocument();
    expect(await screen.findByText("0 passed")).toBeInTheDocument();
    expect(await screen.findByText("56.5%")).toBeInTheDocument();
  });

  it("opens the daily picks view from the dashboard CTA", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));

    const dialog = await screen.findByRole("dialog", { name: /daily picks/i });
    expect(within(dialog).getByRole("heading", { name: /daily picks/i })).toBeInTheDocument();
    expect(within(dialog).getByText("Cumulative hit rate")).toBeInTheDocument();
    expect(within(dialog).getByText("75.0%")).toBeInTheDocument();
    expect(within(dialog).getByText("Validation sample")).toBeInTheDocument();
    expect(within(dialog).getByText("76 picks")).toBeInTheDocument();
  });

  it("keeps the daily picks CTA available after matches load", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));

    const dialog = await screen.findByRole("dialog", { name: /daily picks/i });
    expect(within(dialog).getByRole("heading", { name: /daily picks/i })).toBeInTheDocument();
  });

  it("renders daily picks market filters and recommendation cards", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));

    const dialog = await screen.findByRole("dialog", { name: /daily picks/i });
    expect(within(dialog).getByRole("heading", { name: /daily picks/i })).toBeInTheDocument();
    expect(screen.getByText("HOME")).toBeInTheDocument();
    expect(screen.getByText("DRAW")).toBeInTheDocument();
    expect(screen.queryByText("Home -0.5")).not.toBeInTheDocument();
    expect(screen.queryByText("Under 2.5")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /handicap/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /over\/under/i })).toBeInTheDocument();
  });

  it("opens match detail from a daily pick outside the dashboard league page", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    const dailyPicksDialog = await screen.findByRole("dialog", { name: /daily picks/i });
    fireEvent.change(within(dailyPicksDialog).getByRole("combobox", { name: /league/i }), {
      target: { value: "champions-league" },
    });
    expect(await within(dailyPicksDialog).findByAltText("Inter crest")).toBeInTheDocument();
    expect(within(dailyPicksDialog).getByAltText("Bayern Munich crest")).toBeInTheDocument();
    fireEvent.click(
      await within(dailyPicksDialog).findByRole("button", {
        name: /inter.*bayern munich/i,
      }),
    );

    expect(
      await screen.findByRole("dialog", { name: "Inter vs Bayern Munich" }),
    ).toBeInTheDocument();
    const matchDetailDialog = screen.getByRole("dialog", { name: "Inter vs Bayern Munich" });
    expect(within(matchDetailDialog).getByAltText("Inter crest")).toBeInTheDocument();
    expect(within(matchDetailDialog).getByAltText("Bayern Munich crest")).toBeInTheDocument();
  });

  it("keeps the daily picks modal open when closing a detail opened from it", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    const dailyPicksDialog = await screen.findByRole("dialog", { name: /daily picks/i });
    fireEvent.change(within(dailyPicksDialog).getByRole("combobox", { name: /league/i }), {
      target: { value: "champions-league" },
    });
    fireEvent.click(
      await within(dailyPicksDialog).findByRole("button", {
        name: /inter.*bayern munich/i,
      }),
    );

    expect(
      await screen.findByRole("dialog", { name: "Inter vs Bayern Munich" }),
    ).toBeInTheDocument();

    fireEvent.keyDown(document, { key: "Escape" });

    await waitFor(() => {
      expect(
        screen.queryByRole("dialog", { name: "Inter vs Bayern Munich" }),
      ).not.toBeInTheDocument();
    });
    expect(screen.getByRole("dialog", { name: /daily picks/i })).toBeInTheDocument();
    await waitFor(() => {
      expect(document.body.style.overflow).toBe("hidden");
    });
  });

  it("keeps held moneyline picks as no-bet while detail data is loading", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    fireEvent.click(screen.getByRole("checkbox", { name: /show held/i }));
    fireEvent.click(await screen.findByRole("button", { name: /arsenal vs fulham/i }));

    const dialog = await screen.findByRole("dialog", { name: "Arsenal vs Fulham" });
    expect(within(dialog).getByLabelText("Bet: Unavailable")).toBeInTheDocument();
  });

  it("filters daily picks by market family and can show held items", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    fireEvent.click(await screen.findByRole("button", { name: /handicap/i }));

    expect(screen.queryByText("Home -0.5")).not.toBeInTheDocument();
    expect(screen.queryByText("Under 2.5")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("checkbox", { name: /show held/i }));

    expect(await screen.findByText("Home -0.5")).toBeInTheDocument();
    expect(screen.getByText(/market price only/i)).toBeInTheDocument();
    expect(screen.queryByText(/low confidence/i)).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /moneyline/i }));

    expect(await screen.findByText(/low confidence/i)).toBeInTheDocument();
  });

  it("opens the teaser CTA with all league daily picks and updates cards when the league select changes", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    const dailyPicksDialog = await screen.findByRole("dialog", { name: /daily picks/i });

    expect(
      await within(dailyPicksDialog).findByRole("button", {
        name: /chelsea.*manchester city/i,
      }),
    ).toBeInTheDocument();
    expect(
      await within(dailyPicksDialog).findByRole("button", {
        name: /inter.*bayern munich/i,
      }),
    ).toBeInTheDocument();
    expect(within(dailyPicksDialog).getByRole("combobox", { name: /league/i })).toHaveValue("");

    fireEvent.change(within(dailyPicksDialog).getByRole("combobox", { name: /league/i }), {
      target: { value: "champions-league" },
    });

    await waitFor(() => {
      expect(
        within(dailyPicksDialog).getByRole("button", {
          name: /inter.*bayern munich/i,
        }),
      ).toBeInTheDocument();
    });
    expect(
      within(dailyPicksDialog).queryByRole("button", {
        name: /chelsea.*manchester city/i,
      }),
    ).not.toBeInTheDocument();
  });

  it("resets daily picks filters to the all-leagues context on reopen", async () => {
    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    let dailyPicksDialog = await screen.findByRole("dialog", { name: /daily picks/i });

    fireEvent.change(within(dailyPicksDialog).getByRole("combobox", { name: /league/i }), {
      target: { value: "" },
    });
    fireEvent.click(within(dailyPicksDialog).getByRole("button", { name: /handicap/i }));
    fireEvent.click(within(dailyPicksDialog).getByRole("checkbox", { name: /show held/i }));
    fireEvent.keyDown(document, { key: "Escape" });

    await waitFor(() => {
      expect(screen.queryByRole("dialog", { name: /daily picks/i })).not.toBeInTheDocument();
    });

    fireEvent.click(await screen.findByRole("tab", { name: "UEFA Champions League" }));
    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    dailyPicksDialog = await screen.findByRole("dialog", { name: /daily picks/i });

    expect(within(dailyPicksDialog).getByRole("combobox", { name: /league/i })).toHaveValue("");
    expect(within(dailyPicksDialog).getByRole("checkbox", { name: /show held/i })).not.toBeChecked();
    expect(
      await within(dailyPicksDialog).findByRole("button", {
        name: /inter.*bayern munich/i,
      }),
    ).toBeInTheDocument();
    expect(
      within(dailyPicksDialog).getByRole("button", {
        name: /chelsea.*manchester city/i,
      }),
    ).toBeInTheDocument();
  });

  it("does not refetch daily picks with stale filters on reopen", async () => {
    render(<App />);

    const fetchMock = vi.mocked(fetch);
    const dailyPicksUrls = () =>
      fetchMock.mock.calls
        .map(([url]) => url)
        .filter((url): url is string => typeof url === "string" && url.startsWith("/api/daily-picks"));

    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    const dailyPicksDialog = await screen.findByRole("dialog", { name: /daily picks/i });
    fireEvent.click(within(dailyPicksDialog).getByRole("button", { name: /handicap/i }));
    fireEvent.click(within(dailyPicksDialog).getByRole("checkbox", { name: /show held/i }));
    fireEvent.keyDown(document, { key: "Escape" });

    await waitFor(() => {
      expect(screen.queryByRole("dialog", { name: /daily picks/i })).not.toBeInTheDocument();
    });

    const callsBeforeReopen = dailyPicksUrls().length;
    fireEvent.click(await screen.findByRole("button", { name: /^view$/i }));
    await screen.findByRole("dialog", { name: /daily picks/i });

    await waitFor(() => {
      expect(dailyPicksUrls().slice(callsBeforeReopen).length).toBeGreaterThan(0);
    });
    const reopenCalls = dailyPicksUrls().slice(callsBeforeReopen);

    expect(reopenCalls).not.toContainEqual(expect.stringContaining("marketFamily=spreads"));
    expect(reopenCalls).not.toContainEqual(expect.stringContaining("includeHeld=true"));
  });

  it("does not fetch evaluation reports when opening the match detail modal", async () => {
    render(<App />);

    const fetchMock = vi.mocked(fetch);

    const matchButton = await screen.findByRole("button", {
      name: "Chelsea vs Manchester City",
    });
    fireEvent.click(matchButton);

    await screen.findByRole("dialog", { name: "Chelsea vs Manchester City" });

    const calledUrls = fetchMock.mock.calls.map(([url]) => String(url));
    expect(calledUrls).not.toContain("/api/predictions/source-evaluation/latest");
    expect(calledUrls).not.toContain("/api/predictions/source-evaluation/history");
    expect(calledUrls).not.toContain("/api/predictions/model-registry/latest");
    expect(calledUrls).not.toContain("/api/predictions/fusion-policy/latest");
    expect(calledUrls).not.toContain("/api/predictions/fusion-policy/history");
    expect(calledUrls).not.toContain("/api/reviews/aggregation/latest");
    expect(calledUrls).not.toContain("/api/reviews/aggregation/history");
    expect(calledUrls).not.toContain("/api/rollouts/promotion/latest");
  });

  it("renders league tabs and summary metadata before the match grid", async () => {
    render(<App />);

    const tablist = await screen.findByRole("tablist", { name: "Leagues" });
    expect(within(tablist).getByRole("tab", { name: "Premier League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(
      within(tablist).getByRole("tab", { name: "UEFA Champions League" }),
    ).toBeInTheDocument();
    expect(screen.getAllByText(/3 matches/i).length).toBeGreaterThan(0);
    expect(screen.getByText(/1 need review/i)).toBeInTheDocument();
    expect(screen.getByRole("tabpanel", { name: "Match Timeline" })).toBeInTheDocument();
    expect(screen.getByText("Showing 3 of 3 matches")).toBeInTheDocument();
  });

  it("keeps match view pages cached when switching between upcoming and recent tabs", async () => {
    render(<App />);

    await screen.findByRole("tab", { name: "Upcoming Matches" });
    const fetchMock = vi.mocked(fetch);

    fireEvent.click(screen.getByRole("tab", { name: "Recent Results" }));
    await waitFor(() => {
      expect(
        fetchMock.mock.calls.some(([input]) =>
          String(input).includes("/api/matches?leagueId=premier-league&view=recent"),
        ),
      ).toBe(true);
    });
    const recentRequestCount = fetchMock.mock.calls.filter(([input]) =>
      String(input).includes("/api/matches?leagueId=premier-league&view=recent"),
    ).length;

    fireEvent.click(screen.getByRole("tab", { name: "Upcoming Matches" }));
    fireEvent.click(screen.getByRole("tab", { name: "Recent Results" }));

    await waitFor(() => {
      expect(
        fetchMock.mock.calls.filter(([input]) =>
          String(input).includes("/api/matches?leagueId=premier-league&view=recent"),
        ),
      ).toHaveLength(recentRequestCount);
    });
  });

  it("does not start duplicate match page requests when tab effects are replayed", async () => {
    render(
      <StrictMode>
        <App />
      </StrictMode>,
    );

    await screen.findByRole("tab", { name: "Upcoming Matches" });
    const fetchMock = vi.mocked(fetch);

    fireEvent.click(screen.getByRole("tab", { name: "Recent Results" }));

    await waitFor(() => {
      expect(
        fetchMock.mock.calls.filter(([input]) =>
          String(input).includes("/api/matches?leagueId=premier-league&view=recent"),
        ),
      ).toHaveLength(1);
    });
  });

  it("derives a translated conference league tab when the bootstrap response omits league summaries", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);

        if (url.startsWith("/api/matches")) {
          return {
            ok: true,
            json: async () => ({
              items: [
                {
                  id: "uecl-match-001",
                  leagueId: "conference-league",
                  leagueLabel: "UEFA Conference League",
                  leagueEmblemUrl: "https://crests.football-data.org/UECL.png",
                  homeTeam: "Chelsea",
                  awayTeam: "Fiorentina",
                  kickoffAt: "2026-04-28 19:00 UTC",
                  status: "Scheduled",
                  recommendedPick: null,
                  confidence: null,
                  needsReview: false,
                },
              ],
              leagues: [],
              predictionSummary: null,
              selectedLeagueId: "conference-league",
              nextCursor: null,
              totalMatches: 1,
            }),
          };
        }

        if (url.startsWith("/api/daily-picks")) {
          return {
            ok: true,
            json: async () => ({
              generatedAt: null,
              date: "2026-04-28",
              target: {
                minDailyRecommendations: 5,
                maxDailyRecommendations: 10,
                hitRate: 0.7,
                roi: 0.2,
              },
              coverage: { moneyline: 0, spreads: 0, totals: 0, held: 0 },
              items: [],
              heldItems: [],
            }),
          };
        }

        return {
          ok: true,
          json: async () => ({
            report: null,
            history: [],
            latest: null,
            previous: null,
            reviewAggregation: null,
            rollout: null,
            decision: null,
          }),
        };
      }),
    );

    render(<App />);

    const tablist = await screen.findByRole("tablist", { name: "Leagues" });
    expect(
      within(tablist).getByRole("tab", { name: "UEFA Conference League" }),
    ).toHaveAttribute("aria-selected", "true");
  });

  it("renders a card-grid style match list with operator metadata", async () => {
    render(<App />);

    const matchButton = await screen.findByRole("button", {
      name: "Chelsea vs Manchester City",
    });
    const card = within(matchButton);

    expect(matchButton).toBeInTheDocument();
    expect(card.getAllByText("Review Required").length).toBeGreaterThan(0);
    expect(card.getByLabelText("Predicted: Home")).toBeInTheDocument();
    expect(card.getByLabelText("Actual: Away")).toBeInTheDocument();
    expect(card.getByLabelText("Bet: Recommended")).toBeInTheDocument();
    expect(card.getByLabelText("Verdict: Miss")).toBeInTheDocument();
    expect(card.getByLabelText("Review required")).toBeInTheDocument();
    expect(card.getByText("1")).toBeInTheDocument();
    expect(card.getByText("2")).toBeInTheDocument();
    expect(card.getByAltText("Chelsea crest")).toBeInTheDocument();
    expect(card.getByAltText("Manchester City crest")).toBeInTheDocument();
    expect(card.queryByText("HOME lean with the strongest available support.")).toBeNull();
  });

  it("renders unavailable pick and confidence when no prediction exists yet", async () => {
    render(<App />);

    const matchButton = await screen.findByRole("button", {
      name: "Arsenal vs Fulham",
    });
    const card = within(matchButton);

    expect(card.getByLabelText("Predicted: Unavailable")).toBeInTheDocument();
    expect(card.getByLabelText("Actual: Pending")).toBeInTheDocument();
    expect(card.getByLabelText("Bet: Unavailable")).toBeInTheDocument();
    expect(card.getByLabelText("Verdict: Pending")).toBeInTheDocument();
  });

  it("renders a no-bet card with a separate value pick signal", async () => {
    render(<App />);

    const matchButton = await screen.findByRole("button", {
      name: "Liverpool vs Brentford",
    });
    const card = within(matchButton);

    expect(card.getByLabelText("Predicted: Home")).toBeInTheDocument();
    expect(card.getByLabelText("Bet: No bet")).toBeInTheDocument();
    expect(card.getByLabelText("Verdict: Pending")).toBeInTheDocument();
    expect(card.getByText("Value Pick")).toBeInTheDocument();
    expect(card.queryByText("Derived Markets")).toBeNull();
    expect(card.queryByText("spreads")).toBeNull();
    expect(card.queryByText("totals")).toBeNull();
  });

  it("marks the card button as selected to prepare the modal flow", async () => {
    render(<App />);

    const chelseaMatchButton = await screen.findByRole("button", {
      name: "Chelsea vs Manchester City",
    });
    const liverpoolMatchButton = await screen.findByRole("button", {
      name: "Liverpool vs Brentford",
    });

    expect(chelseaMatchButton).toHaveAttribute("aria-pressed", "false");
    expect(liverpoolMatchButton).toHaveAttribute("aria-pressed", "false");

    fireEvent.click(chelseaMatchButton);

    expect(chelseaMatchButton).toHaveAttribute("aria-pressed", "true");
    expect(liverpoolMatchButton).toHaveAttribute("aria-pressed", "false");
  });

  it("renders confidence breakdown details inside the prediction modal", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    const dialog = await screen.findByRole("dialog", { name: "Chelsea vs Manchester City" });
    const modal = within(dialog);
    expect(modal.getByLabelText("Predicted: Home")).toBeInTheDocument();
    expect(modal.getByLabelText("Actual: Away")).toBeInTheDocument();
    expect(modal.getByLabelText("Bet: Recommended")).toBeInTheDocument();
    expect(modal.getByLabelText("Verdict: Miss")).toBeInTheDocument();
    expect(await screen.findByText("Confidence Breakdown")).toBeInTheDocument();
    expect(screen.getByText("Raw score")).toBeInTheDocument();
    expect(screen.getByText("76%")).toBeInTheDocument();
    expect(screen.getAllByText("Source agreement").length).toBeGreaterThan(0);
    expect(screen.getAllByText("100%").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Baseline source").length).toBeGreaterThan(0);
    expect(screen.getAllByText("trained baseline").length).toBeGreaterThan(0);
    expect(screen.getByText("Signal drivers")).toBeInTheDocument();
    expect(screen.getByText("Top factors")).toBeInTheDocument();
    expect(screen.getByText("strength edge · home 0.42")).toBeInTheDocument();
    expect(screen.getByText("xG proxy · home 0.31")).toBeInTheDocument();
    expect(screen.getByText("strength edge")).toBeInTheDocument();
    expect(screen.getByText("xG proxy")).toBeInTheDocument();
    expect(screen.getByText("schedule edge")).toBeInTheDocument();
    expect(screen.getByText("lineup edge")).toBeInTheDocument();
    expect(screen.getByText("Home lineup score")).toBeInTheDocument();
    expect(screen.getByText("Away lineup score")).toBeInTheDocument();
    expect(screen.getByText("Lineup source")).toBeInTheDocument();
    expect(screen.getByText("1.82")).toBeInTheDocument();
    expect(screen.getByText("1.21")).toBeInTheDocument();
    expect(
      screen.getByText("espn lineups + recent starters + pl missing players"),
    ).toBeInTheDocument();
    expect(screen.getByText("Calibration evidence")).toBeInTheDocument();
    expect(screen.getByText("0.7-0.8")).toBeInTheDocument();
    expect(screen.getByText("67% hit rate · 6 matches")).toBeInTheDocument();
    expect(screen.queryByText("Market price")).toBeNull();
    expect(screen.queryByText("+40%")).toBeNull();
    expect(modal.getAllByText("Additional markets").length).toBeGreaterThan(0);
    expect(modal.getByText("Handicap 0.5")).toBeInTheDocument();
    expect(modal.getByText("Home -0.5 lead · 54% vs 46%")).toBeInTheDocument();
    expect(modal.getByText("Over/Under 2.5")).toBeInTheDocument();
    expect(modal.getByText("Under 2.5 lead · 57% vs 43%")).toBeInTheDocument();
    expect(modal.queryByText("L: -0.5")).toBeNull();
    expect(modal.queryByText("L: 2.5")).toBeNull();
  });

  it("renders the modal summary surfaces for the selected match", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    const dialog = await screen.findByRole("dialog", { name: "Chelsea vs Manchester City" });
    const modal = within(dialog);

    expect(modal.getByLabelText("Predicted: Home")).toBeInTheDocument();
    expect(modal.getByLabelText("Bet: Recommended")).toBeInTheDocument();
    expect(modal.getByText("Signal summary")).toBeInTheDocument();
    expect(modal.getByText("HOME lean with the strongest available support.")).toBeInTheDocument();
    expect(modal.queryByText("Value Pick")).toBeNull();
    expect(modal.getAllByText("Additional markets").length).toBeGreaterThan(0);
    expect(modal.getByRole("button", { name: /View Full Intelligence Report/ })).toBeInTheDocument();
  });

  it("renders phase 6 history, shadow, and rollout comparison surfaces when history payloads are present", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    fireEvent.click(
      await screen.findByRole("button", { name: /View Full Intelligence Report/ }),
    );

    expect(await screen.findByText("Current vs Previous")).toBeInTheDocument();
    expect(screen.getAllByText("Source history").length).toBeGreaterThan(0);
    expect(screen.getByText("Current fused hit rate")).toBeInTheDocument();
    expect(screen.getByText("75% vs 50%")).toBeInTheDocument();
    expect(screen.getByText("Shadow lane")).toBeInTheDocument();
    expect(screen.getByText("Shadow evaluation in progress")).toBeInTheDocument();
    expect(screen.getByText("Rollout lane")).toBeInTheDocument();
    expect(screen.getByText("Rollout increased to 25%")).toBeInTheDocument();
    expect(screen.getByText("Fusion policy history")).toBeInTheDocument();
    expect(screen.getByText("Candidate weights outperform baseline")).toBeInTheDocument();
    expect(screen.getByText("Review aggregation trend")).toBeInTheDocument();
    expect(screen.getByText("12 vs 9")).toBeInTheDocument();
    expect(screen.getByText("Review aggregation rollout at 50%")).toBeInTheDocument();
  });

  it("switches leagues with keyboard and shows the selected league content", async () => {
    render(<App />);

    const premierLeagueTab = await screen.findByRole("tab", {
      name: "Premier League",
    });
    fireEvent.keyDown(premierLeagueTab, { key: "ArrowRight" });

    expect(await screen.findByRole("tab", { name: "UEFA Champions League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );

    const matchButton = screen.getByRole("button", { name: "Inter vs Bayern Munich" });
    expect(matchButton).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Chelsea vs Manchester City" })).toBeNull();
    expect(screen.getByRole("tab", { name: "UEFA Champions League" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    const card = within(matchButton);
    expect(card.getByLabelText("Predicted: Draw")).toBeInTheDocument();
    expect(card.getByLabelText("Actual: Draw")).toBeInTheDocument();
    expect(card.getByLabelText("Bet: Recommended")).toBeInTheDocument();
    expect(card.getByLabelText("Verdict: Correct")).toBeInTheDocument();
    expect(card.queryByText("DRAW lean with the strongest available support.")).toBeNull();
  });

  it("shows league match and review counts above the match list", async () => {
    render(<App />);

    expect(await screen.findByText("3 matches")).toBeInTheDocument();
    expect(screen.getByText("1 need review")).toBeInTheDocument();

    fireEvent.keyDown(
      screen.getByRole("tab", { name: "Premier League" }),
      { key: "ArrowRight" },
    );

    expect(await screen.findByText("1 matches")).toBeInTheDocument();
    expect(screen.getByText("1 need review")).toBeInTheDocument();
  });

  it("opens a match detail modal from the match card", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
      ).toBeInTheDocument();
    });
    expect(screen.getByText(/Recommended Pick/i)).toBeInTheDocument();
    expect(screen.getByText("View Full Intelligence Report")).toBeInTheDocument();
  });

  it("keeps predicted outcome visible for no-bet matches across dashboard, modal, and report", async () => {
    render(<App />);

    const matchButton = await screen.findByRole("button", {
      name: "Liverpool vs Brentford",
    });
    const card = within(matchButton);

    expect(card.getByLabelText("Predicted: Home")).toBeInTheDocument();
    expect(matchButton.querySelector(".matchCard.state-no-bet")).not.toBeNull();

    fireEvent.click(matchButton);

    const dialog = await screen.findByRole("dialog", {
      name: "Liverpool vs Brentford",
    });
    const modal = within(dialog);

    expect(modal.getByLabelText("Predicted: Home")).toBeInTheDocument();
    expect(modal.getAllByText("58%").length).toBeGreaterThan(0);
    expect(dialog.classList.contains("state-no-bet")).toBe(true);

    fireEvent.click(
      modal.getByRole("button", { name: /View Full Intelligence Report/ }),
    );

    expect(await screen.findByLabelText("Predicted: Home")).toBeInTheDocument();
    expect(screen.getByLabelText("Bet: No bet")).toBeInTheDocument();
    expect(document.querySelector(".reportHero-noBet")).not.toBeNull();
  });

  it("uses different card tones for pending and settled no-bet states", () => {
    const pendingNoBetMatch: MatchCardRow = {
      id: "pending-no-bet",
      leagueId: "premier-league",
      homeTeam: "Liverpool",
      awayTeam: "Brentford",
      kickoffAt: "2026-04-27 21:00 UTC",
      status: "Prediction Ready",
      recommendedPick: null,
      confidence: null,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.58,
        recommended: false,
        noBetReason: "low_confidence",
      },
      needsReview: false,
    };
    const settledNoBetMatch: MatchCardRow = {
      ...pendingNoBetMatch,
      id: "settled-no-bet",
      status: "Review Ready",
      finalResult: "HOME",
      homeScore: 2,
      awayScore: 1,
    };

    const { rerender } = render(
      <MatchCard match={pendingNoBetMatch} isSelected={false} onOpen={() => {}} />,
    );

    expect(document.querySelector(".matchCard.state-no-bet")).not.toBeNull();
    expect(document.querySelector(".matchCard.state-complete")).toBeNull();

    rerender(
      <MatchCard match={settledNoBetMatch} isSelected={false} onOpen={() => {}} />,
    );

    expect(document.querySelector(".matchCard.state-complete")).not.toBeNull();
  });

  it("renders settled displayed predictions as hits even when the bet lane stayed on no-bet", () => {
    const settledNoBetMatch: MatchCardRow = {
      id: "settled-no-bet-hit",
      leagueId: "premier-league",
      homeTeam: "Liverpool",
      awayTeam: "Brentford",
      kickoffAt: "2026-04-27 21:00 UTC",
      status: "Review Ready",
      finalResult: "HOME",
      homeScore: 2,
      awayScore: 1,
      recommendedPick: null,
      confidence: null,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.58,
        recommended: false,
        noBetReason: "low_confidence",
      },
      needsReview: false,
    };

    render(
      <MatchCard match={settledNoBetMatch} isSelected={false} onOpen={() => {}} />,
    );

    const card = within(
      screen.getByRole("button", { name: "Liverpool vs Brentford" }),
    );

    expect(card.getByLabelText("Verdict: Correct")).toBeInTheDocument();
    expect(document.querySelector(".verdictGlyph-hit")).not.toBeNull();
    expect(document.querySelector(".verdictText-hit")).not.toBeNull();
  });

  it("uses matching modal tones for pending and settled no-bet states", () => {
    const pendingNoBetMatch: MatchCardRow = {
      id: "pending-no-bet",
      leagueId: "premier-league",
      homeTeam: "Liverpool",
      awayTeam: "Brentford",
      kickoffAt: "2026-04-27 21:00 UTC",
      status: "Prediction Ready",
      recommendedPick: null,
      confidence: null,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.58,
        recommended: false,
        noBetReason: "low_confidence",
      },
      needsReview: false,
    };
    const settledNoBetMatch: MatchCardRow = {
      ...pendingNoBetMatch,
      id: "settled-no-bet",
      status: "Review Ready",
      finalResult: "HOME",
      homeScore: 2,
      awayScore: 1,
    };

    const { rerender } = render(
      <MatchDetailModal
        match={pendingNoBetMatch}
        isOpen
        prediction={null}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    expect(document.querySelector(".detailModal.state-no-bet")).not.toBeNull();
    expect(document.querySelector(".detailModal.state-complete")).toBeNull();

    rerender(
      <MatchDetailModal
        match={settledNoBetMatch}
        isOpen
        prediction={null}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    expect(document.querySelector(".detailModal.state-complete")).not.toBeNull();
  });

  it("localizes the favored variant market summary in Korean", async () => {
    await i18n.changeLanguage("ko");

    const match: MatchCardRow = {
      id: "variant-market-ko",
      leagueId: "premier-league",
      homeTeam: "Liverpool",
      awayTeam: "Brentford",
      kickoffAt: "2026-04-27 21:00 UTC",
      status: "Prediction Ready",
      recommendedPick: null,
      confidence: null,
      needsReview: false,
    };
    const prediction: PredictionSummary = {
      matchId: "variant-market-ko",
      checkpointLabel: "T-12H",
      homeWinProbability: 58,
      drawProbability: 24,
      awayWinProbability: 18,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.58,
        recommended: false,
        noBetReason: "low_confidence",
      },
      variantMarkets: [
        {
          marketFamily: "totals",
          sourceName: "polymarket_totals",
          lineValue: 2.5,
          selectionALabel: "Over 2.5",
          selectionAPrice: 0.43,
          selectionBLabel: "Under 2.5",
          selectionBPrice: 0.57,
          marketSlug: "totals-ko",
        },
      ],
    };

    render(
      <MatchDetailModal
        match={match}
        isOpen
        prediction={prediction}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    const dialog = screen.getByRole("dialog", { name: "Liverpool vs Brentford" });
    const modal = within(dialog);

    expect(modal.getByText("추가 시장")).toBeInTheDocument();
    expect(modal.getByText("언더/오버 2.5")).toBeInTheDocument();
    expect(modal.getByText("언더 2.5 우세 · 57% vs 43%")).toBeInTheDocument();
  });

  it("preserves informative modal copy when only one variant side price is present", () => {
    const match: MatchCardRow = {
      id: "variant-market-partial",
      leagueId: "premier-league",
      homeTeam: "Liverpool",
      awayTeam: "Brentford",
      kickoffAt: "2026-04-27 21:00 UTC",
      status: "Prediction Ready",
      recommendedPick: null,
      confidence: null,
      needsReview: false,
    };
    const prediction: PredictionSummary = {
      matchId: "variant-market-partial",
      checkpointLabel: "T-12H",
      homeWinProbability: 58,
      drawProbability: 24,
      awayWinProbability: 18,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.58,
        recommended: false,
        noBetReason: "low_confidence",
      },
      variantMarkets: [
        {
          marketFamily: "totals",
          sourceName: "polymarket_totals",
          lineValue: 2.5,
          selectionALabel: "Over 2.5",
          selectionAPrice: null,
          selectionBLabel: "Under 2.5",
          selectionBPrice: 0.57,
          marketSlug: "totals-partial",
        },
      ],
    };

    render(
      <MatchDetailModal
        match={match}
        isOpen
        prediction={prediction}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    const dialog = screen.getByRole("dialog", { name: "Liverpool vs Brentford" });
    const modal = within(dialog);

    expect(modal.getByText("Additional markets")).toBeInTheDocument();
    expect(modal.getByText("Over/Under 2.5")).toBeInTheDocument();
    expect(modal.getByText("Under 2.5 · 57%")).toBeInTheDocument();
    expect(modal.queryByText("Over 2.5 vs Under 2.5")).toBeNull();
  });

  it("renders a neutral modal summary when both variant prices are tied", () => {
    const match: MatchCardRow = {
      id: "variant-market-tied",
      leagueId: "premier-league",
      homeTeam: "Liverpool",
      awayTeam: "Brentford",
      kickoffAt: "2026-04-27 21:00 UTC",
      status: "Prediction Ready",
      recommendedPick: null,
      confidence: null,
      needsReview: false,
    };
    const prediction: PredictionSummary = {
      matchId: "variant-market-tied",
      checkpointLabel: "T-12H",
      homeWinProbability: 58,
      drawProbability: 24,
      awayWinProbability: 18,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.58,
        recommended: false,
        noBetReason: "low_confidence",
      },
      variantMarkets: [
        {
          marketFamily: "totals",
          sourceName: "polymarket_totals",
          lineValue: 2.5,
          selectionALabel: "Over 2.5",
          selectionAPrice: 0.5,
          selectionBLabel: "Under 2.5",
          selectionBPrice: 0.5,
          marketSlug: "totals-tied",
        },
      ],
    };

    render(
      <MatchDetailModal
        match={match}
        isOpen
        prediction={prediction}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    const dialog = screen.getByRole("dialog", { name: "Liverpool vs Brentford" });
    const modal = within(dialog);

    expect(modal.getByText("Over 2.5 vs Under 2.5")).toBeInTheDocument();
    expect(modal.queryByText("Over 2.5 lead · 50% vs 50%")).toBeNull();
  });

  it("falls back to localized generic labels for unsupported variant markets in Korean", async () => {
    await i18n.changeLanguage("ko");

    const match: MatchCardRow = {
      id: "variant-market-fallback-ko",
      leagueId: "premier-league",
      homeTeam: "Liverpool",
      awayTeam: "Brentford",
      kickoffAt: "2026-04-27 21:00 UTC",
      status: "Prediction Ready",
      recommendedPick: null,
      confidence: null,
      needsReview: false,
    };
    const prediction: PredictionSummary = {
      matchId: "variant-market-fallback-ko",
      checkpointLabel: "T-12H",
      homeWinProbability: 58,
      drawProbability: 24,
      awayWinProbability: 18,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.58,
        recommended: false,
        noBetReason: "low_confidence",
      },
      variantMarkets: [
        {
          marketFamily: "btts",
          sourceName: "polymarket_btts",
          lineValue: null,
          selectionALabel: "Yes",
          selectionAPrice: 0.6,
          selectionBLabel: "No",
          selectionBPrice: 0.4,
          marketSlug: "btts-ko",
        },
      ],
    };

    render(
      <MatchDetailModal
        match={match}
        isOpen
        prediction={prediction}
        checkpoints={[]}
        review={null}
        onClose={() => {}}
        onOpenReport={() => {}}
      />,
    );

    const dialog = screen.getByRole("dialog", { name: "Liverpool vs Brentford" });
    const modal = within(dialog);

    expect(modal.getAllByText("추가 시장").length).toBeGreaterThan(0);
    expect(modal.getByText("선택지 A 우세 · 60% vs 40%")).toBeInTheDocument();
    expect(modal.queryByText("Yes lead · 60% vs 40%")).toBeNull();
  });

  it("opens a full report view from the detail modal", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );
    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: /View Full Intelligence Report/ }),
      ).toBeInTheDocument();
    });
    fireEvent.click(
      screen.getByRole("button", { name: /View Full Intelligence Report/ }),
    );

    expect(screen.getByRole("heading", { name: "Chelsea" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Manchester City" })).toBeInTheDocument();
    expect(screen.getByText("Intelligence Summary")).toBeInTheDocument();
    expect(screen.getByText("Intelligence Report")).toBeInTheDocument();
    expect(screen.getAllByText("1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("2").length).toBeGreaterThan(0);
    expect(screen.getByText("Prediction vs Actual")).toBeInTheDocument();
    expect(screen.getByText("Predicted outcome")).toBeInTheDocument();
    expect(screen.getByText("Actual outcome")).toBeInTheDocument();
    expect(screen.getByText("Miss type")).toBeInTheDocument();
    expect(screen.getByText("Market verdict")).toBeInTheDocument();
    expect(screen.getAllByText("Home").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Away").length).toBeGreaterThan(0);
    expect(screen.getAllByText("major directional miss").length).toBeGreaterThan(0);
    expect(screen.getByText("Market outperformed model")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText("Calibration evidence")).toBeInTheDocument();
      expect(screen.getByText("67% hit rate · 6 matches")).toBeInTheDocument();
      expect(screen.getByText("Source Performance")).toBeInTheDocument();
      expect(screen.getByText("Checkpoint performance")).toBeInTheDocument();
      expect(screen.getByText("LINEUP CONFIRMED")).toBeInTheDocument();
      expect(screen.getAllByText("Prediction market segment").length).toBeGreaterThan(0);
      expect(screen.getAllByText("with prediction market").length).toBeGreaterThan(0);
    });
  });

  it("does not label report misses as correct calls when review tags are unavailable", () => {
    const match: MatchCardRow = {
      id: "report-miss-without-review",
      leagueId: "premier-league",
      homeTeam: "Chelsea",
      awayTeam: "Manchester City",
      kickoffAt: "2026-04-27 19:00 UTC",
      status: "Review Ready",
      finalResult: "AWAY",
      homeScore: 1,
      awayScore: 2,
      recommendedPick: "HOME",
      confidence: 0.7,
      needsReview: true,
    };
    const prediction: PredictionSummary = {
      matchId: match.id,
      checkpointLabel: "LINEUP_CONFIRMED",
      recommendedPick: "HOME",
      confidence: 0.7,
      homeWinProbability: 70,
      drawProbability: 18,
      awayWinProbability: 12,
      mainRecommendation: {
        pick: "HOME",
        confidence: 0.7,
        recommended: true,
        noBetReason: null,
      },
    };

    render(
      <FullReportView
        match={match}
        prediction={prediction}
        evaluationReport={null}
        evaluationHistoryView={null}
        modelRegistryReport={null}
        fusionPolicyReport={null}
        fusionPolicyHistoryView={null}
        reviewAggregationReport={null}
        reviewAggregationHistoryView={null}
        promotionDecisionReport={null}
        checkpoints={[]}
        review={null}
        onBack={() => {}}
      />,
    );

    expect(screen.getByText("Miss type")).toBeInTheDocument();
    expect(screen.getByText("Miss")).toBeInTheDocument();
    expect(screen.queryByText("Correct call")).toBeNull();
  });

  it("closes the detail modal on Escape", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
      ).toBeInTheDocument();
    });

    fireEvent.keyDown(document, { key: "Escape" });

    expect(
      screen.queryByRole("dialog", { name: "Chelsea vs Manchester City" }),
    ).toBeNull();
  });

  it("closes the detail modal when the backdrop is clicked", async () => {
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "Chelsea vs Manchester City",
      }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole("dialog", { name: "Chelsea vs Manchester City" }),
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("match-detail-backdrop"));

    expect(
      screen.queryByRole("dialog", { name: "Chelsea vs Manchester City" }),
    ).toBeNull();
  });
});

describe("client-assisted validation", () => {
  it("does not show the operator panel unless enabled", () => {
    render(<App />);
    expect(screen.queryByText("Client Validation Jobs")).toBeNull();
  });
});
