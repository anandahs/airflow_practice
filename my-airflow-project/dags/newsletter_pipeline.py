import os
from airflow.sdk import asset, ObjectStoragePath


@asset(schedule="@daily")
def raw_zen_quotes() -> list[dict]:
    """extract random set of quotes"""
    import requests

    response = requests.get("https://zenquotes.io/api/quotes/random")

    return response.json()

@asset(schedule=[raw_zen_quotes]) # triggers when raw_zen_quotes is updated
def selected_quotes(context:dict) -> dict:
    """select 3 quotes: short, medium-length, and long"""

    import numpy as np

    raw = context['ti'].xcom_pull(
            dag_id="raw_zen_quotes",
            task_ids="raw_zen_quotes",
            key="return_value",
            include_prior_dates=True,
            )

    # xcom_pull may wrap the asset's return value in an extra list
    if raw and isinstance(raw[0], list):
        raw = raw[0]

    counts = [int(q["c"]) for q in raw]
    median = np.median(counts)

    median_q = min(raw, key=lambda q: abs(int(q["c"]) - median))
    raw.pop(raw.index(median_q))
    short_q  = [q for q in raw if int(q["c"]) < median][0]
    long_q   = [q for q in raw if int(q["c"]) > median][0]

    return {"short_q": short_q, "median_q": median_q, "long_q": long_q}


STORAGE          = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
CONN_ID          = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
PATH             = os.getenv("OBJECT_STORAGE_PATH_NEWSLETTER", default="include/newsletter")
RECIPIENT_EMAIL  = os.getenv("NEWSLETTER_RECIPIENT_EMAIL", default="")

@asset(schedule=[selected_quotes])
def formatted_newsletter(context: dict) -> None:
    """Fills the newsletter template with today's selected quotes."""

    path = ObjectStoragePath(f"{STORAGE}://{PATH}", conn_id=CONN_ID)

    # Use run date from context — NOT datetime.now() — for idempotency
    date = context["dag_run"].run_after.strftime("%Y-%m-%d")

    quotes = context["ti"].xcom_pull(
        dag_id="selected_quotes",
        task_ids="selected_quotes",
        key="return_value",
        include_prior_dates=True,
    )

    if quotes and isinstance(quotes[0], dict):
        quotes = quotes[0]

    template = (path / "newsletter_template.txt").read_text()

    newsletter = template.format(
        date=date,
        quote_text_1=quotes["short_q"]["q"],   quote_author_1=quotes["short_q"]["a"],
        quote_text_2=quotes["median_q"]["q"],  quote_author_2=quotes["median_q"]["a"],
        quote_text_3=quotes["long_q"]["q"],    quote_author_3=quotes["long_q"]["a"],
    )

    (path / f"{date}_newsletter.txt").write_text(newsletter)


@asset(schedule=[formatted_newsletter])
def email_newsletter(context: dict) -> None:
    """Emails the day's newsletter to the configured recipient."""
    import smtplib
    from email.mime.text import MIMEText

    path = ObjectStoragePath(f"{STORAGE}://{PATH}", conn_id=CONN_ID)
    date = context["dag_run"].run_after.strftime("%Y-%m-%d")
    body = (path / f"{date}_newsletter.txt").read_text()

    smtp_host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", 587))
    smtp_user = os.environ["AIRFLOW__SMTP__SMTP_USER"]
    smtp_pass = os.environ["AIRFLOW__SMTP__SMTP_PASSWORD"]

    msg = MIMEText(body)
    msg["Subject"] = f"Daily Zen Newsletter — {date}"
    msg["From"]    = smtp_user
    msg["To"]      = RECIPIENT_EMAIL

    with smtplib.SMTP(smtp_host, smtp_port) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(smtp_user, smtp_pass)
        smtp.sendmail(smtp_user, [RECIPIENT_EMAIL], msg.as_string())
