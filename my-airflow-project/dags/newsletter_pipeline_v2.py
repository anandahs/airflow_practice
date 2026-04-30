from datetime import datetime, timedelta
from airflow.sdk import dag, task

@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)
def newsletter_pipeline_v2():

    @task
    def raw_zen_quotes() -> list[dict]:
        import requests
        r = requests.get("https://zenquotes.io/api/quotes/random")
        r.raise_for_status()
        return r.json()

    @task
    def selected_quotes(raw: list[dict]) -> dict:
        import numpy as np
        counts   = [int(q["c"]) for q in raw]
        median   = np.median(counts)
        median_q = min(raw, key=lambda q: abs(int(q["c"]) - median))
        raw.pop(raw.index(median_q))
        short_q  = [q for q in raw if int(q["c"]) < median][0]
        long_q   = [q for q in raw if int(q["c"]) > median][0]
        return {"short_q": short_q, "median_q": median_q, "long_q": long_q}

    @task
    def get_user_info() -> list[dict]:
        return [
            {"id": 1, "name": "Alice", "location": "New York"},
            {"id": 2, "name": "Bob",   "location": "London"},
        ]

    @task(retries=5)  # extra retries for external API call
    def get_weather(user: dict) -> dict:
        import requests
        url = (
            f"https://geocoding-api.open-meteo.com/v1/search"
            f"?name={user['location']}&count=1"
        )
        geo = requests.get(url).json()["results"][0]
        weather = requests.get(
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={geo['latitude']}&longitude={geo['longitude']}"
            f"&current_weather=true"
        ).json()["current_weather"]
        return {**user, "temp": weather["temperature"]}

    @task
    def send_newsletter(quotes: dict, user_weather: dict) -> None:
        print(
            f"Sending to {user_weather['name']}: "
            f"{quotes['median_q']['q']} | Weather: {user_weather['temp']}°C"
        )

    quotes = selected_quotes(raw_zen_quotes())
    users  = get_user_info()
    weather = get_weather.expand(user=users)

    # pass quotes to every personalized newsletter
    send_newsletter.expand_kwargs([
        {"quotes": quotes, "user_weather": uw} for uw in [weather]
    ])

newsletter_pipeline_v2()