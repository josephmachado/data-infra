
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots

def show_visuals(spark):
    # Read the data
    df = spark.sql("select * from stackoverflow.activation_rate").toPandas()
    df['cohort_month'] = pd.to_datetime(df['cohort_month'])

    # Create figure with subplots
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))
    fig.suptitle('StackOverflow User Activation Analysis (2008-2024)', fontsize=16, fontweight='bold')

    # Plot 1: Activation Rate Over Time
    ax1 = axes[0]
    ax1.plot(df['cohort_month'], df['activation_rate_pct'], linewidth=2, color='#2E86AB')
    ax1.axvspan(datetime(2008, 7, 1), datetime(2012, 1, 1), alpha=0.2, color='green', label='Golden Age (75% avg)')
    ax1.axvspan(datetime(2012, 5, 1), datetime(2016, 1, 1), alpha=0.2, color='orange', label='Steep Decline (33% avg)')
    ax1.axvspan(datetime(2020, 1, 1), datetime(2024, 4, 1), alpha=0.2, color='red', label='Recent Era (11% avg)')
    ax1.axhline(y=75.24, color='green', linestyle='--', alpha=0.5, linewidth=1)
    ax1.axhline(y=19.10, color='black', linestyle='--', alpha=0.5, linewidth=1, label='Overall Avg (19.1%)')
    ax1.set_ylabel('Activation Rate (%)', fontsize=12, fontweight='bold')
    ax1.set_title('User Activation Rate Trend', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper right')
    ax1.set_ylim(0, 90)

    # Plot 2: Monthly User Signups
    ax2 = axes[1]
    ax2.bar(df['cohort_month'], df['total_users'], width=20, color='#A23B72', alpha=0.7)
    ax2.set_ylabel('Total New Users', fontsize=12, fontweight='bold')
    ax2.set_title('Monthly User Signups', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')

    # Highlight key events
    ax2.annotate('May 2012 Surge\n(46% increase)', 
                xy=(datetime(2012, 5, 1), 57051), 
                xytext=(datetime(2013, 1, 1), 250000),
                arrowprops=dict(arrowstyle='->', color='red', lw=2),
                fontsize=10, color='red', fontweight='bold')

    ax2.annotate('COVID Spike\n(April 2020)', 
                xy=(datetime(2020, 4, 1), 259740), 
                xytext=(datetime(2019, 1, 1), 350000),
                arrowprops=dict(arrowstyle='->', color='red', lw=2),
                fontsize=10, color='red', fontweight='bold')

    ax2.annotate('March 2024\nAnomaly', 
                xy=(datetime(2024, 3, 1), 414736), 
                xytext=(datetime(2022, 6, 1), 450000),
                arrowprops=dict(arrowstyle='->', color='red', lw=2),
                fontsize=10, color='red', fontweight='bold')

    # Plot 3: Activated Users Over Time
    ax3 = axes[2]
    ax3.plot(df['cohort_month'], df['activated_users'], linewidth=2, color='#F18F01', label='Activated Users')
    ax3.set_ylabel('Activated Users', fontsize=12, fontweight='bold')
    ax3.set_xlabel('Cohort Month', fontsize=12, fontweight='bold')
    ax3.set_title('Monthly Activated Users', fontsize=13, fontweight='bold')
    ax3.grid(True, alpha=0.3)

    # Format x-axis for all plots
    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator(2))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.tight_layout()

def get_visualization(spark):
    activation_rate_df = spark.sql("""
    select cohort_month
    , total_users
    , activation_rate_pct
    from stackoverflow.activation_rate 
    order by cohort_month 
    """).toPandas()


    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add bar chart for total_users (primary y-axis)
    fig.add_trace(
        go.Bar(x=activation_rate_df['cohort_month'], 
               y=activation_rate_df['total_users'],
               name='Total Users'),
        secondary_y=False
    )
    
    # Add line chart for activation_rate_pct (secondary y-axis)
    fig.add_trace(
        go.Scatter(x=activation_rate_df['cohort_month'], 
                   y=activation_rate_df['activation_rate_pct'],
                   name='Activation Rate %',
                   mode='lines+markers',
                   line=dict(color='red', width=2),
                   marker=dict(size=8)),
        secondary_y=True
    )
    
    # Update layout
    fig.update_layout(
        title='Total Users and Activation Rate by Cohort Month',
        xaxis_title='Cohort Month'
    )
    
    # Set y-axes titles
    fig.update_yaxes(title_text="Total Users", secondary_y=False)
    fig.update_yaxes(title_text="Activation Rate %", secondary_y=True)

    return fig

