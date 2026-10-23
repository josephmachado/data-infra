import altair as alt
import pandas as pd
from pyspark.sql import SparkSession

OUTPUT_DIR = "./"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("generate_charts").master("local[*]").getOrCreate()
    )


# ── Chart 1: Ad Funnel ────────────────────────────────────────────────────────


def generate_funnel_chart(spark: SparkSession) -> None:
    funnel_df = spark.sql("""
        SELECT
            SUM(total_impressions) AS impressions,
            SUM(total_clicks)      AS clicks,
            SUM(total_conversions) AS conversions
        FROM local.gold.ad_funnel_summary
    """).toPandas()

    funnel_long = pd.DataFrame(
        {
            "stage": ["Impressions", "Clicks", "Conversions"],
            "count": [
                funnel_df["impressions"].iloc[0],
                funnel_df["clicks"].iloc[0],
                funnel_df["conversions"].iloc[0],
            ],
            "order": [1, 2, 3],
        }
    ).assign(
        pct=lambda x: (x["count"] / x["count"].iloc[0] * 100).round(1),
        label=lambda x: x.apply(lambda r: f"{r['count']:,.0f}  ({r['pct']}%)", axis=1),
    )

    bars = (
        alt.Chart(funnel_long)
        .mark_bar()
        .encode(
            x=alt.X(
                "pct:Q",
                title="% of Impressions",
                scale=alt.Scale(domain=[0, 105]),
                axis=alt.Axis(format=".1f"),
            ),
            y=alt.Y(
                "stage:N", sort=["Impressions", "Clicks", "Conversions"], title=None
            ),
            color=alt.Color(
                "stage:N",
                scale=alt.Scale(
                    domain=["Impressions", "Clicks", "Conversions"],
                    range=["#1D4ED8", "#3B82F6", "#93C5FD"],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("stage:N", title="Stage"),
                alt.Tooltip("count:Q", title="Count", format=","),
                alt.Tooltip("pct:Q", title="% of Impressions", format=".1f"),
            ],
        )
    )

    labels = (
        alt.Chart(funnel_long)
        .mark_text(align="left", dx=5, fontSize=12, fontWeight="bold", color="#1F2937")
        .encode(
            x=alt.X("pct:Q"),
            y=alt.Y("stage:N", sort=["Impressions", "Clicks", "Conversions"]),
            text=alt.Text("label:N"),
        )
    )

    chart = (
        (bars + labels)
        .properties(
            width=600,
            height=200,
            title=alt.TitleParams(
                text="Ad Funnel — Impressions → Clicks → Conversions",
                subtitle="Last 90 days | % of total impressions at each stage",
                fontSize=16,
                subtitleFontSize=12,
                anchor="start",
            ),
        )
        .configure_axis(
            labelFontSize=11, titleFontSize=12, grid=True, gridColor="#F3F4F6"
        )
        .configure_view(strokeWidth=0)
    )

    chart.save(f"{OUTPUT_DIR}/funnel.png")
    print("✓ funnel.png saved")


# ── Chart 2: ROAS Over Time ───────────────────────────────────────────────────


def generate_roas_chart(spark: SparkSession) -> None:
    roas_df = spark.sql("""
        SELECT
            event_date,
            SUM(total_spend)                              AS total_spend,
            SUM(total_revenue)                            AS total_revenue,
            SUM(l7d_revenue) / NULLIF(SUM(l7d_spend), 0) AS l7d_roas,
            AVG(l30d_roas)                                AS l30d_roas
        FROM local.gold.ad_campaign_performance
        GROUP BY event_date
        ORDER BY event_date
    """).toPandas()

    roas_long = roas_df.melt(
        id_vars=["event_date"],
        value_vars=["l7d_roas", "l30d_roas"],
        var_name="window",
        value_name="roas",
    ).assign(
        window=lambda x: x["window"].map(
            {
                "l7d_roas": "7-day ROAS",
                "l30d_roas": "30-day ROAS",
            }
        ),
        event_date=lambda x: pd.to_datetime(x["event_date"]),
        roas=lambda x: x["roas"].astype(float).round(2),
    )

    breakeven = (
        alt.Chart(pd.DataFrame({"roas": [1.0]}))
        .mark_rule(strokeDash=[6, 4], color="#EF4444", strokeWidth=1.5)
        .encode(y="roas:Q")
    )

    breakeven_label = (
        alt.Chart(pd.DataFrame({"roas": [1.0], "label": ["Break-even (1.0)"]}))
        .mark_text(align="left", dx=4, dy=-8, fontSize=11, color="#EF4444")
        .encode(y="roas:Q", text="label:N")
    )

    lines = (
        alt.Chart(roas_long)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X(
                "event_date:T",
                title="Date",
                axis=alt.Axis(format="%b %Y", tickCount="month", labelAngle=-45),
            ),
            y=alt.Y(
                "roas:Q",
                title="ROAS",
                axis=alt.Axis(format=".1f"),
                scale=alt.Scale(zero=False),
            ),
            color=alt.Color(
                "window:N",
                title="Rolling Window",
                scale=alt.Scale(
                    domain=["7-day ROAS", "30-day ROAS"], range=["#93C5FD", "#1D4ED8"]
                ),
                legend=alt.Legend(orient="top"),
            ),
            opacity=alt.condition(
                alt.datum.window == "7-day ROAS", alt.value(0.6), alt.value(1.0)
            ),
            tooltip=[
                alt.Tooltip("event_date:T", title="Date", format="%b %d %Y"),
                alt.Tooltip("window:N", title="Window"),
                alt.Tooltip("roas:Q", title="ROAS", format=".2f"),
            ],
        )
        .properties(
            width=600,
            height=350,
            title=alt.TitleParams(
                text="ROAS Over Time — All Campaigns (Full Year 2025)",
                subtitle="7-day rolling shows volatility. 30-day rolling shows trend. Red line = break-even.",
                fontSize=16,
                subtitleFontSize=12,
                anchor="start",
            ),
        )
    )

    chart = (
        (lines + breakeven + breakeven_label)
        .configure_axis(
            labelFontSize=11, titleFontSize=12, grid=True, gridColor="#F3F4F6"
        )
        .configure_view(strokeWidth=0)
    )

    chart.save(f"{OUTPUT_DIR}/roas_over_time.png")
    print("✓ roas_over_time.png saved")


# ── Main ──────────────────────────────────────────────────────────────────────


def run() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("Generating charts...")
    generate_funnel_chart(spark)
    generate_roas_chart(spark)
    print("All charts saved to ./")

    spark.stop()


if __name__ == "__main__":
    run()
