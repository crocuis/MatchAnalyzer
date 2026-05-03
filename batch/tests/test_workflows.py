from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def read_workflow(name: str) -> str:
    return (REPO_ROOT / ".github" / "workflows" / name).read_text()


def test_ingest_fixtures_workflow_sets_real_fixture_date() -> None:
    workflow = read_workflow("ingest-fixtures.yml")

    assert 'cron: "0 1,13 * * *"' in workflow
    assert 'cron: "0 */6 * * *"' not in workflow
    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "BSD_API_KEY:" not in workflow
    assert "REAL_FIXTURE_SYNC_MODE: schedule" in workflow
    assert "BSD_LINEUP_LOOKAHEAD_HOURS" not in workflow
    assert "REAL_FIXTURE_DATE=" in workflow


def test_ingest_fixtures_workflow_syncs_current_day_and_second_week_window() -> None:
    workflow = read_workflow("ingest-fixtures.yml")

    assert "FIXTURE_FUTURE_START_DAYS: 7" in workflow
    assert "FIXTURE_FUTURE_END_DAYS: 14" in workflow
    assert 'date -u -d "$TARGET_DATE_CANONICAL" +%F' in workflow
    assert "seq \"$FIXTURE_FUTURE_START_DAYS\" \"$FIXTURE_FUTURE_END_DAYS\"" in workflow
    assert "FIXTURE_DATES<<EOF" in workflow
    assert "while IFS= read -r TARGET_DATE; do" in workflow
    assert 'REAL_FIXTURE_DATE="$TARGET_DATE" python3 -m batch.src.jobs.ingest_fixtures_job' in workflow
    assert "CHANGED_MATCH_IDS_FILE" not in workflow
    assert "REAL_PREDICTION_MATCH_IDS" not in workflow
    assert "backfill_external_prediction_signals_job" not in workflow
    assert 'python3 -m batch.src.jobs.run_predictions_job' not in workflow
    assert "PRIMARY_FIXTURE_DATE" not in workflow
    assert "<<'PY'" not in workflow


def test_ingest_markets_workflow_sets_real_market_date() -> None:
    workflow = read_workflow("ingest-markets.yml")

    assert 'cron: "15 */8 * * *"' in workflow
    assert 'cron: "15 */4 * * *"' not in workflow
    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "ODDS_API_KEY: ${{ secrets.ODDS_API_KEY }}" in workflow
    assert "BSD_API_KEY: ${{ secrets.BSD_API_KEY }}" in workflow
    assert "BSD_LINEUP_LOOKAHEAD_HOURS: 48" in workflow
    assert "MARKET_CHECKPOINT_TYPES: T_MINUS_24H,T_MINUS_6H,T_MINUS_1H,LINEUP_CONFIRMED" in workflow
    assert "REAL_MARKET_DATE=" in workflow
    assert "Sync market target snapshots" in workflow
    assert 'PREDICTION_SYNC_TARGET_DATE="$REAL_MARKET_DATE"' in workflow
    assert 'PREDICTION_SYNC_TARGET_CHECKPOINT_TYPES="$MARKET_CHECKPOINT_TYPES"' in workflow
    assert "python3 -m batch.src.jobs.sync_prediction_checkpoints_job" in workflow
    assert "Changed market match ids: $CHANGED_MATCH_IDS" in workflow
    assert "Prediction refresh is intentionally deferred" in workflow
    assert "REAL_PREDICTION_MATCH_IDS" not in workflow
    assert "backfill_external_prediction_signals_job" not in workflow
    assert '--match-ids "$CHANGED_MATCH_IDS"' not in workflow
    assert "--clubelo-date-stride-days 1" not in workflow
    assert 'python3 -m batch.src.jobs.run_predictions_job' not in workflow
    assert "<<'PY'" not in workflow
    assert "python3 -c" in workflow


def test_run_predictions_workflow_supports_manual_targets_and_optional_llm_run() -> None:
    workflow = read_workflow("run-predictions.yml")

    assert "schedule:" not in workflow
    assert "# 12:00 Asia/Seoul" not in workflow
    assert 'cron: "0 3 * * *"' not in workflow
    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "target_match_ids:" in workflow
    assert "enable_llm_advisory:" in workflow
    assert "llm_provider:" in workflow
    assert "llm_model:" in workflow
    assert "REAL_PREDICTION_DATE=" in workflow
    assert "REAL_PREDICTION_MATCH_IDS=" in workflow
    assert "LLM_PREDICTION_ADVISORY_ENABLED=1" in workflow
    assert "LLM_PROVIDER=$LLM_PROVIDER_INPUT" in workflow
    assert "LLM_PREDICTION_MODEL=$LLM_MODEL_INPUT" in workflow
    assert "NVIDIA_API_KEY: ${{ secrets.NVIDIA_API_KEY }}" in workflow
    assert "OPENROUTER_API_KEY: ${{ secrets.OPENROUTER_API_KEY }}" in workflow
    assert "R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}" in workflow
    assert "R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}" in workflow
    assert "R2_S3_ENDPOINT: ${{ secrets.R2_S3_ENDPOINT }}" in workflow
    assert '[ "${{ github.event_name }}" = "schedule" ]' not in workflow
    assert "DAILY_PICK_ARTIFACT_ENABLED=0" in workflow
    assert "DAILY_PICK_ARTIFACT_ENABLED=1" in workflow
    assert "MATCH_ANALYZER_DISABLE_DAILY_PICK_TRACKING_SYNC=1" in workflow
    assert "DAILY_PICK_SYNC_DATE=" not in workflow
    assert "DAILY_PICK_ARTIFACT_DATE=" in workflow
    assert "Backfill external prediction signals" in workflow
    assert "backfill_external_prediction_signals_job" in workflow
    assert '--match-ids "$REAL_PREDICTION_MATCH_IDS"' in workflow
    assert '--kickoff-date "$REAL_PREDICTION_DATE"' in workflow
    assert "--clubelo-date-stride-days 1" in workflow
    assert "python3 -m batch.src.jobs.export_daily_pick_artifacts_job" in workflow
    assert "if: ${{ env.DAILY_PICK_ARTIFACT_ENABLED == '1' }}" in workflow


def test_sync_prediction_checkpoints_workflow_targets_due_matches_and_daily_pick_dates() -> None:
    workflow = read_workflow("sync-prediction-checkpoints.yml")

    assert 'cron: "5 * * * *"' in workflow
    assert "PREDICTION_SYNC_LOOKBACK_MINUTES:" in workflow
    assert "github.event.inputs.lookback_minutes || '60'" in workflow
    assert "LLM_PREDICTION_ADVISORY_ENABLED:" in workflow
    assert "python3 -m batch.src.jobs.sync_prediction_checkpoints_job" in workflow
    assert "SYNC_TARGET_MATCH_IDS" in workflow
    assert "SYNC_EXTERNAL_SIGNAL_MATCH_IDS" in workflow
    assert "SYNC_DAILY_PICK_DATES" in workflow
    assert "No due prediction checkpoints detected; skipping prediction refresh." in workflow
    assert "backfill_external_prediction_signals_job" in workflow
    assert '--match-ids "$SYNC_EXTERNAL_SIGNAL_MATCH_IDS"' in workflow
    assert "REAL_PREDICTION_MATCH_IDS=\"$SYNC_TARGET_MATCH_IDS\"" in workflow
    assert "MATCH_ANALYZER_DISABLE_DAILY_PICK_TRACKING_SYNC=1" in workflow
    assert "No daily-pick prediction checkpoints changed; skipping daily pick refresh." in workflow
    assert "DAILY_PICK_SYNC_DATE=\"$TARGET_DATE\"" in workflow
    assert "DAILY_PICK_ARTIFACT_DATE=\"$TARGET_DATE\"" in workflow


def test_post_match_review_workflow_sets_real_review_date_and_daily_llm_run() -> None:
    workflow = read_workflow("post-match-review.yml")

    assert 'cron: "45 */6 * * *"' not in workflow
    assert 'cron: "20 4 * * *"' in workflow
    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "enable_llm_review:" in workflow
    assert "llm_provider:" in workflow
    assert "llm_model:" in workflow
    assert "REAL_REVIEW_DATE=" in workflow
    assert "LLM_REVIEW_ADVISORY_ENABLED=1" in workflow
    assert "LLM_PROVIDER=$LLM_PROVIDER_INPUT" in workflow
    assert "LLM_REVIEW_MODEL=$LLM_MODEL_INPUT" in workflow
    assert "NVIDIA_API_KEY: ${{ secrets.NVIDIA_API_KEY }}" in workflow
    assert "OPENROUTER_API_KEY: ${{ secrets.OPENROUTER_API_KEY }}" in workflow
    assert "R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}" in workflow
    assert "R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}" in workflow
    assert "R2_S3_ENDPOINT: ${{ secrets.R2_S3_ENDPOINT }}" in workflow
    assert '[ "${{ github.event.schedule }}" = "20 4 * * *" ]' in workflow
    assert "date -u -d 'yesterday' +%F" in workflow
    assert 'DAILY_PICK_SETTLE_DATE="$REAL_REVIEW_DATE"' in workflow
    assert 'DAILY_PICK_ARTIFACT_DATE="$REAL_REVIEW_DATE"' in workflow
    assert "python3 -m batch.src.jobs.run_daily_pick_tracking_job" in workflow
    assert "python3 -m batch.src.jobs.export_daily_pick_artifacts_job" in workflow


def test_report_missing_signal_coverage_workflow_runs_daily() -> None:
    workflow = read_workflow("report-missing-signal-coverage.yml")

    assert "schedule:" in workflow
    assert 'cron: "10 5 * * *"' in workflow
    assert "workflow_dispatch:" in workflow
    assert "workflow_run:" not in workflow


def test_report_missing_signal_coverage_workflow_validates_and_exports_target_date() -> None:
    workflow = read_workflow("report-missing-signal-coverage.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert 'if [ "${{ github.event_name }}" != "workflow_dispatch" ]; then' in workflow
    assert "REPORT_ARGS=" in workflow
    assert "TARGET_DATE_INPUT" in workflow
    assert "*$'\\n'*" in workflow
    assert "*$'\\r'*" in workflow
    assert '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' in workflow
    assert 'TARGET_DATE_CANONICAL="$(date -u -d "$TARGET_DATE_INPUT" +%F 2>/dev/null || true)"' in workflow
    assert '[ "$TARGET_DATE_CANONICAL" != "$TARGET_DATE_INPUT" ]' in workflow
    assert "printf 'REPORT_ARGS=\\n' >> \"$GITHUB_ENV\"" in workflow
    assert "printf 'REPORT_ARGS=--target-date %s\\n' \"$TARGET_DATE_CANONICAL\" >> \"$GITHUB_ENV\"" in workflow
    assert 'python3 -m batch.src.jobs.report_missing_signal_coverage_job $REPORT_ARGS' in workflow


def test_deploy_production_workflow_waits_for_main_ci_and_runs_ordered_deploy_steps() -> None:
    workflow = read_workflow("deploy-production.yml")

    assert "workflow_run:" in workflow
    assert "workflows: [test]" in workflow
    assert "branches: [main]" in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "environment: production" in workflow
    assert "npm install" in workflow
    assert "npm ci" not in workflow
    assert "Apply Postgres migrations" in workflow
    assert "python3 scripts/apply_postgres_migrations.py" in workflow
    assert "Smoke check Neon database" in workflow
    assert "wrangler secret put DATABASE_URL" in workflow
    assert "supabase db push" not in workflow
    assert "npm run deploy:api" in workflow
    assert "wrangler pages secret put MATCH_ANALYZER_API_ORIGIN" in workflow
    assert "npm run deploy:web" in workflow
    assert "Smoke check production endpoints" in workflow


def test_test_workflow_avoids_duplicate_workspace_runs_without_lockfile_cache() -> None:
    workflow = read_workflow("test.yml")

    assert 'cache: "npm"' not in workflow
    assert "npm test" in workflow
    assert "npm --workspace apps/api run test" not in workflow
    assert "npm --workspace apps/web run test" not in workflow


def test_sync_match_results_workflow_runs_every_two_hours_and_reviews_changed_dates() -> None:
    workflow = read_workflow("sync-match-results.yml")

    assert 'cron: "35 */2 * * *"' in workflow
    assert "RESULT_SYNC_DELAY_HOURS:" in workflow
    assert "github.event.inputs.delay_hours || '2'" in workflow
    assert "RESULT_SYNC_LOOKBACK_HOURS:" in workflow
    assert "github.event.inputs.lookback_hours || '48'" in workflow
    assert "python3 -m batch.src.jobs.sync_match_results_job" in workflow
    assert "SYNC_CHANGED_DATES" in workflow
    assert "No changed match results detected; skipping review refresh." in workflow
    assert "REAL_REVIEW_DATE=\"$TARGET_DATE\"" in workflow
    assert 'LLM_REVIEW_ADVISORY_ENABLED: "0"' in workflow
    assert "python3 -m batch.src.jobs.run_post_match_review_job" in workflow
    assert "python3 -m batch.src.jobs.run_daily_pick_tracking_job" in workflow
    assert "python3 -m batch.src.jobs.export_daily_pick_artifacts_job" in workflow
    assert "Collect pending Betman watchlist dates" in workflow
    assert "BETMAN_WATCHLIST_DATES" in workflow
    assert "python3 -m batch.src.jobs.report_daily_pick_segment_quality_job --pending-dates-only" in workflow
    assert "Retry pending Betman watchlist settlements" in workflow
    assert "BETMAN_WATCHLIST_SETTLE_DATE=$TARGET_DATE" in workflow
    assert "Report Betman watchlist quality" in workflow
    assert "python3 -m batch.src.jobs.report_daily_pick_segment_quality_job --candidate-limit 10" in workflow


def test_deploy_production_workflow_documents_required_production_secrets() -> None:
    workflow = read_workflow("deploy-production.yml")

    assert "DATABASE_URL" in workflow
    assert "CLOUDFLARE_API_TOKEN" in workflow
    assert "CLOUDFLARE_ACCOUNT_ID" in workflow
    assert "CLOUDFLARE_PAGES_PROJECT_NAME" in workflow
    assert "VITE_API_BASE_URL" in workflow
    assert "OPERATIONAL_REPORTS_API_KEY" in workflow
    assert "SUPABASE_ACCESS_TOKEN" not in workflow
    assert "SUPABASE_PROJECT_ID" not in workflow
    assert "SUPABASE_DB_PASSWORD" not in workflow
    assert "VITE_SUPABASE_URL" not in workflow
    assert "VITE_SUPABASE_PUBLISHABLE_KEY" not in workflow


def test_verify_workflow_uses_npm_install_without_lockfile_dependency() -> None:
    workflow = read_workflow("test.yml")

    assert "npm install" in workflow
    assert "npm ci" not in workflow
