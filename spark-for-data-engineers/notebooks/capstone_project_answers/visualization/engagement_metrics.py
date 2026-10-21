import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.facecolor'] = 'white'

def show_visuals(spark):
    df_tags = spark.sql("""
                        SELECT 
                        tag,
                        COUNT(*) as question_count,
                        ROUND(AVG(engagement_velocity_score), 2) as avg_engagement_velocity,
                        ROUND(AVG(ViewCount), 0) as avg_views,
                        ROUND(AVG(AnswerCount), 2) as avg_answers,
                        ROUND(AVG(Score), 2) as avg_score,
                        ROUND(AVG(hours_old), 2) as avg_age_hours
                    FROM stackoverflow.engagement_velocity
                    LATERAL VIEW EXPLODE(SPLIT(Tags, '\\\\|')) t1 AS tag
                    GROUP BY tag
                    HAVING COUNT(*) >= 100  -- Filter for statistical significance
                    ORDER BY avg_engagement_velocity DESC
                    LIMIT 20
                        """).toPandas()
    
    # Categorize tags
    def categorize_tag(tag):
        if 'faq' in tag.lower():
            return 'FAQ'
        elif 'git' in tag.lower() or tag in ['remote-branch', 'fast-forward', 'undo']:
            return 'Git'
        else:
            return 'Other'
    
    df_tags['category'] = df_tags['tag'].apply(categorize_tag)
    
    # Calculate statistics
    git_tags = df_tags[df_tags['category'] == 'Git']
    faq_tags = df_tags[df_tags['category'] == 'FAQ']
    other_tags = df_tags[df_tags['category'] == 'Other']
    
    # Create visualization
    fig = plt.figure(figsize=(16, 10))
    
    # ========================================================================
    # Plot 1: Top Tags by Engagement (Colored by Category)
    # ========================================================================
    ax1 = plt.subplot(2, 2, (1, 2))
    
    df_top15 = df_tags.head(15)
    category_colors = {'Git': '#2ca02c', 'FAQ': '#d62728', 'Other': '#7f7f7f'}
    colors_list = [category_colors[cat] for cat in df_top15['category']]
    
    bars = ax1.barh(range(len(df_top15)), df_top15['avg_engagement_velocity'], 
                    color=colors_list, alpha=0.8, edgecolor='black', linewidth=1.5)
    
    ax1.set_yticks(range(len(df_top15)))
    ax1.set_yticklabels(df_top15['tag'], fontsize=11)
    ax1.set_xlabel('Average Engagement Velocity', fontsize=13, fontweight='bold')
    ax1.invert_yaxis()
    ax1.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (idx, row) in enumerate(df_top15.iterrows()):
        ax1.text(float(row['avg_engagement_velocity']) + 0.04, i, 
                f"{row['avg_engagement_velocity']:.2f}", 
                va='center', fontsize=10, fontweight='bold')
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=category_colors['Git'], label=f'Git ({len(git_tags)} tags, {len(git_tags)/len(df_tags)*100:.0f}%)'),
        Patch(facecolor=category_colors['FAQ'], label=f'FAQ ({len(faq_tags)} tags, {len(faq_tags)/len(df_tags)*100:.0f}%)'),
        Patch(facecolor=category_colors['Other'], label=f'Other ({len(other_tags)} tags, {len(other_tags)/len(df_tags)*100:.0f}%)')
    ]
    ax1.legend(handles=legend_elements, loc='lower right', fontsize=11, framealpha=0.9)
    
    # ========================================================================
    # Plot 2: Category Performance Comparison
    # ========================================================================
    ax2 = plt.subplot(2, 2, 3)
    
    category_stats = df_tags.groupby('category').agg({
        'avg_engagement_velocity': 'mean',
        'avg_views': 'mean',
        'question_count': 'sum'
    }).round(2)
    
    categories = ['FAQ', 'Git', 'Other']  # Order for better visualization
    category_stats = category_stats.reindex(categories)
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars1 = ax2.bar(x - width/2, category_stats['avg_engagement_velocity'], width, 
                    label='Avg Engagement Velocity', color='#1f77b4', alpha=0.8, 
                    edgecolor='black', linewidth=1.5)
    
    ax2_right = ax2.twinx()
    bars2 = ax2_right.bar(x + width/2, category_stats['avg_views']/1000, width, 
                          label='Avg Views (thousands)', color='#ff7f0e', alpha=0.8, 
                          edgecolor='black', linewidth=1.5)
    
    ax2.set_xlabel('Content Category', fontsize=13, fontweight='bold')
    ax2.set_ylabel('Engagement Velocity', fontsize=12, fontweight='bold', color='#1f77b4')
    ax2_right.set_ylabel('Avg Views (thousands)', fontsize=12, fontweight='bold', color='#ff7f0e')
    ax2.set_title('Category Performance: FAQ > Git > Other', fontsize=13, fontweight='bold', pad=15)
    ax2.set_xticks(x)
    ax2.set_xticklabels(categories, fontsize=12, fontweight='bold')
    ax2.tick_params(axis='y', labelcolor='#1f77b4')
    ax2_right.tick_params(axis='y', labelcolor='#ff7f0e')
    
    # Add value labels
    for bar in bars1:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    for bar in bars2:
        height = bar.get_height()
        ax2_right.text(bar.get_x() + bar.get_width()/2., height,
                      f'{height:.0f}K', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_right.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=10)
    ax2.grid(axis='y', alpha=0.3)
    
    # ========================================================================
    # Plot 3: Views Distribution by Category
    # ========================================================================
    ax3 = plt.subplot(2, 2, 4)
    
    category_data = []
    category_labels = []
    
    for cat in ['FAQ', 'Git', 'Other']:
        cat_df = df_tags[df_tags['category'] == cat]
        if len(cat_df) > 0:
            category_data.append(cat_df['avg_views'].values)
            category_labels.append(f"{cat}\n(n={len(cat_df)})")
    
    bp = ax3.boxplot(category_data, labels=category_labels, patch_artist=True,
                     showmeans=True, meanline=True)
    
    # Color the boxes
    colors_box = ['#d62728', '#2ca02c', '#7f7f7f']
    for patch, color in zip(bp['boxes'], colors_box):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax3.set_ylabel('Average Views', fontsize=12, fontweight='bold')
    ax3.set_title('View Count Distribution by Category\n(FAQ content gets 4x more views)', 
                  fontsize=13, fontweight='bold', pad=15)
    ax3.grid(axis='y', alpha=0.3)
    ax3.set_yscale('log')
    
    plt.tight_layout()
