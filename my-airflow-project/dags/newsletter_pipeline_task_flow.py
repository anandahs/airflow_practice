import os
from datetime import datetime
from airflow.sdk import dag, task, ObjectStoragePath

STORAGE         = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
CONN_ID         = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
PATH            = os.getenv("OBJECT_STORAGE_PATH_NEWSLETTER", default="include/newsletter")
RECIPIENT_EMAIL = os.getenv("NEWSLETTER_RECIPIENT_EMAIL", default="")


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)
def newsletter_taskflow_pipeline():

    @task
    def raw_zen_quotes() -> list[dict]:
        """Extract random set of quotes."""
        import requests
        response = requests.get("https://zenquotes.io/api/quotes/random")
        return response.json()

    @task
    def selected_quotes(raw: list[dict]) -> dict:
        """Select 3 quotes: short, medium-length, and long."""
        import numpy as np

        counts   = [int(q["c"]) for q in raw]
        median   = np.median(counts)
        median_q = min(raw, key=lambda q: abs(int(q["c"]) - median))
        raw.pop(raw.index(median_q))
        short_q  = [q for q in raw if int(q["c"]) < median][0]
        long_q   = [q for q in raw if int(q["c"]) > median][0]

        return {"short_q": short_q, "median_q": median_q, "long_q": long_q}

    @task
    def formatted_newsletter(quotes: dict, **context) -> str:
        """Fills the newsletter template with today's selected quotes."""
        path = ObjectStoragePath(f"{STORAGE}://{PATH}", conn_id=CONN_ID)
        date = context["dag_run"].run_after.strftime("%Y-%m-%d")

        template   = (path / "newsletter_template.txt").read_text()
        newsletter = template.format(
            date=date,
            quote_text_1=quotes["short_q"]["q"],  quote_author_1=quotes["short_q"]["a"],
            quote_text_2=quotes["median_q"]["q"], quote_author_2=quotes["median_q"]["a"],
            quote_text_3=quotes["long_q"]["q"],   quote_author_3=quotes["long_q"]["a"],
        )

        (path / f"{date}_newsletter.txt").write_text(newsletter)
        return newsletter  # pass content directly to next task

    @task
    def email_newsletter(body: str, **context) -> None:
        """Emails the day's newsletter to the configured recipient."""
        import smtplib
        from email.mime.text import MIMEText

        date = context["dag_run"].run_after.strftime("%Y-%m-%d")

        smtp_host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", 587))
        smtp_user = os.environ["AIRFLOW__SMTP__SMTP_USER"]
        smtp_pass = os.environ["AIRFLOW__SMTP__SMTP_PASSWORD"]

        msg            = MIMEText(body, _charset="utf-8")
        msg["Subject"] = f"Daily Zen Newsletter — {date}"
        msg["From"]    = smtp_user
        msg["To"]      = RECIPIENT_EMAIL

        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(smtp_user, smtp_pass)
            smtp.sendmail(smtp_user, [RECIPIENT_EMAIL], msg.as_string())

    # Wire tasks together — data flows automatically
    raw       = raw_zen_quotes()
    quotes    = selected_quotes(raw)
    newsletter = formatted_newsletter(quotes)
    email_newsletter(newsletter)


newsletter_taskflow_pipeline()