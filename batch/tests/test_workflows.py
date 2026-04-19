from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def read_workflow(name: str) -> str:
    return (REPO_ROOT / ".github" / "workflows" / name).read_text()


def test_ingest_fixtures_workflow_sets_real_fixture_date() -> None:
    workflow = read_workflow("ingest-fixtures.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "REAL_FIXTURE_DATE=" in workflow


def test_ingest_markets_workflow_sets_real_market_date() -> None:
    workflow = read_workflow("ingest-markets.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "REAL_MARKET_DATE=" in workflow


def test_run_predictions_workflow_sets_real_prediction_date() -> None:
    workflow = read_workflow("run-predictions.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "REAL_PREDICTION_DATE=" in workflow


def test_post_match_review_workflow_sets_real_review_date() -> None:
    workflow = read_workflow("post-match-review.yml")

    assert "workflow_dispatch:" in workflow
    assert "target_date:" in workflow
    assert "REAL_REVIEW_DATE=" in workflow


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
