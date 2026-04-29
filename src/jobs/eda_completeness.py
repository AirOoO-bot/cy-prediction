import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def calculate_null_percentage(file_path):
    """Loads a file and calculates the percentage of missing values."""
    if not os.path.exists(file_path):
        print(f"Warning: File not found at {file_path}")
        return 0
    
    # Load data based on extension
    if file_path.endswith('.csv'):
        # ADD IT HERE ⬇️
        df = pd.read_csv(file_path, low_memory=False)
    elif 'parquet' in file_path:
        df = pd.read_parquet(file_path)
    else:
        return 0
    
    return (df.isnull().sum().sum() / df.size) * 100

def run_eda_completeness():
    # 1. Define the paths based on your folder structure
    datasets = {
        'Crops': {
            'raw': 'raw_data/crops.csv',
            'clean': 'cleaned_data/crops_clean_parquet'
        },
        'Soil': {
            'raw': 'raw_data/soil.csv',
            'clean': 'cleaned_data/soil_clean_parquet'
        },
        'Weather': {
            'raw': 'raw_data/weather.csv',
            'clean': 'cleaned_data/weather_clean_parquet'
        }
    }

    # 2. Gather the actual percentages
    plot_data = []
    for name, paths in datasets.items():
        raw_null = calculate_null_percentage(paths['raw'])
        clean_null = calculate_null_percentage(paths['clean'])
        
        plot_data.append({'Dataset': name, 'Missing %': raw_null, 'Status': 'Raw (Before)'})
        plot_data.append({'Dataset': name, 'Missing %': clean_null, 'Status': 'Cleaned (After)'})

    df_plot = pd.DataFrame(plot_data)

    # 3. Visual Setup
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))
    
    ax = sns.barplot(
        x='Dataset', 
        y='Missing %', 
        hue='Status', 
        data=df_plot, 
        palette={'Raw (Before)': '#e74c3c', 'Cleaned (After)': '#2ecc71'}
    )

    # 4. Formatting
    plt.title('Data Quality Improvement: Before vs After Pre-processing', fontsize=14, fontweight='bold')
    plt.ylabel('Total Missing Values (%)')
    plt.ylim(0, max(df_plot['Missing %'].max() + 5, 20)) # Ensure scale is readable

    # Add labels on top of bars
    for p in ax.patches:
        ax.annotate(f'{p.get_height():.1f}%', 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha='center', va='center', xytext=(0, 7), 
                    textcoords='offset points', fontsize=10)

    # 5. Save to EDA folder
    os.makedirs('EDA', exist_ok=True)
    plt.savefig('EDA/eda_data_completeness.png')
    print("Graph saved to EDA/eda_data_completeness.png")

if __name__ == "__main__":
    run_eda_completeness()