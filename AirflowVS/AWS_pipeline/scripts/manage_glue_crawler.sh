#!/usr/bin/env bash
set -euo pipefail

CRAWLER_NAME="${CRAWLER_NAME:-airflow_s3_crawler_silver}"
DATABASE_NAME="${DATABASE_NAME:-silver_db}"
ROLE_NAME="${ROLE_NAME:-sparkaccess}"
S3_TARGET_PATH="${S3_TARGET_PATH:-s3://airflow-aws-ananda/silver/obt_parquet/}"

usage() {
  cat <<EOF
Usage: $(basename "$0") create|run|destroy

Creates, runs, or deletes the Glue crawler '${CRAWLER_NAME}'
which catalogs '${S3_TARGET_PATH}' into the '${DATABASE_NAME}' database.

Examples:
  ./scripts/manage_glue_crawler.sh create
  ./scripts/manage_glue_crawler.sh run
  ./scripts/manage_glue_crawler.sh destroy
EOF
}

ensure_database() {
  if aws glue get-database --name "$DATABASE_NAME" >/dev/null 2>&1; then
    echo "Database '$DATABASE_NAME' already exists."
  else
    aws glue create-database --database-input "{\"Name\":\"${DATABASE_NAME}\"}" >/dev/null
    echo "Created database '$DATABASE_NAME'."
  fi
}

create_crawler() {
  ensure_database

  if aws glue get-crawler --name "$CRAWLER_NAME" >/dev/null 2>&1; then
    echo "Crawler '$CRAWLER_NAME' already exists."
  else
    aws glue create-crawler \
      --name "$CRAWLER_NAME" \
      --role "$ROLE_NAME" \
      --database-name "$DATABASE_NAME" \
      --targets "{\"S3Targets\":[{\"Path\":\"${S3_TARGET_PATH}\"}]}" \
      --schema-change-policy "UpdateBehavior=UPDATE_IN_DATABASE,DeleteBehavior=DEPRECATE_IN_DATABASE" \
      --recrawl-policy "RecrawlBehavior=CRAWL_EVERYTHING" >/dev/null
    echo "Created crawler '$CRAWLER_NAME'."
  fi
}

run_crawler() {
  if ! aws glue get-crawler --name "$CRAWLER_NAME" >/dev/null 2>&1; then
    echo "Crawler '$CRAWLER_NAME' does not exist. Run '$(basename "$0") create' first." >&2
    exit 1
  fi

  aws glue start-crawler --name "$CRAWLER_NAME" >/dev/null
  echo "Started crawler '$CRAWLER_NAME'."
}

destroy_crawler() {
  if ! aws glue get-crawler --name "$CRAWLER_NAME" >/dev/null 2>&1; then
    echo "Crawler '$CRAWLER_NAME' does not exist."
    exit 0
  fi

  aws glue delete-crawler --name "$CRAWLER_NAME" >/dev/null
  echo "Deleted crawler '$CRAWLER_NAME'."
}

case "${1:-}" in
  create)
    create_crawler
    ;;
  run)
    run_crawler
    ;;
  destroy)
    destroy_crawler
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
