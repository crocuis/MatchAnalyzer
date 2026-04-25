from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def read_workflow(name: str) -> str:
    return (REPO_ROOT / ".github" / "workflows" / name).read_text()


def test_ingest_fixtures_workflow_sets_real_fixture_date() -> None:
    workflow = read_workflow("ingest-fixtures.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
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
    assert "REAL_PREDICTION_MATCH_IDS" in workflow
    assert 'python3 -m batch.src.jobs.run_predictions_job' in workflow
    assert 'echo "PRIMARY_FIXTURE_DATE=$TARGET_DATE_CANONICAL"' in workflow
    assert 'if [ "$TARGET_DATE" != "$PRIMARY_FIXTURE_DATE" ]; then' in workflow
    assert "<<'PY'" not in workflow
    assert "python3 -c" in workflow


def test_ingest_markets_workflow_sets_real_market_date() -> None:
    workflow = read_workflow("ingest-markets.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "REAL_MARKET_DATE=" in workflow
    assert "REAL_PREDICTION_MATCH_IDS" in workflow
    assert 'python3 -m batch.src.jobs.run_predictions_job' in workflow
    assert "<<'PY'" not in workflow
    assert "python3 -c" in workflow


def test_run_predictions_workflow_supports_manual_date_or_match_targets_only() -> None:
    workflow = read_workflow("run-predictions.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "target_match_ids:" in workflow
    assert "REAL_PREDICTION_DATE=" in workflow
    assert "REAL_PREDICTION_MATCH_IDS=" in workflow
    assert "schedule:" not in workflow


def test_post_match_review_workflow_sets_real_review_date() -> None:
    workflow = read_workflow("post-match-review.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "REAL_REVIEW_DATE=" in workflow
    assert "date -u -d 'yesterday' +%F" in workflow


def test_report_missing_signal_coverage_workflow_runs_after_predictions() -> None:
    workflow = read_workflow("report-missing-signal-coverage.yml")

    assert "workflow_run:" in workflow
    assert "workflows: [ingest-fixtures, ingest-markets, run-predictions]" in workflow
    assert "types: [completed]" in workflow
    assert "workflow_dispatch:" in workflow
    assert "github.event_name == 'workflow_dispatch'" in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "schedule:" not in workflow


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
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "environment: production" in workflow
    assert "npm install" in workflow
    assert "npm ci" not in workflow
    assert "supabase db push" in workflow
    assert "npm run deploy:api" in workflow
    assert "npm run deploy:web" in workflow
    assert "Smoke check production endpoints" in workflow


def test_deploy_production_workflow_documents_required_production_secrets() -> None:
    workflow = read_workflow("deploy-production.yml")

    assert "SUPABASE_ACCESS_TOKEN" in workflow
    assert "SUPABASE_PROJECT_ID" in workflow
    assert "SUPABASE_DB_PASSWORD" in workflow
    assert "CLOUDFLARE_API_TOKEN" in workflow
    assert "CLOUDFLARE_ACCOUNT_ID" in workflow
    assert "CLOUDFLARE_PAGES_PROJECT_NAME" in workflow
    assert "VITE_API_BASE_URL" in workflow
    assert "VITE_SUPABASE_URL" in workflow
    assert "VITE_SUPABASE_PUBLISHABLE_KEY" in workflow


def test_verify_workflow_uses_npm_install_without_lockfile_dependency() -> None:
    workflow = read_workflow("test.yml")

    assert "npm install" in workflow
    assert "npm ci" not in workflow
