import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
import seaborn as sns

def create_weather_comfort_score(df):
    """
    Create weather comfort score based on temperature, precipitation, and humidity
    """
    return df.withColumn(
        'weather_comfort_score',
        when((col('temperatura_maxima') >= 18) & (col('temperatura_maxima') <= 28) &
             (col('prob_precipitacion_00_24') <= 30) &
             (col('humedad_relativa_maxima') <= 70), 100)
        .when((col('temperatura_maxima') >= 15) & (col('temperatura_maxima') <= 32) &
              (col('prob_precipitacion_00_24') <= 50), 75)
        .when((col('temperatura_maxima') >= 10) & (col('temperatura_maxima') <= 35) &
              (col('prob_precipitacion_00_24') <= 70), 50)
        .otherwise(25)
    )

def create_seasonal_score(df):
    """
    Create seasonal attractiveness score
    """
    return df.withColumn(
        'seasonal_score',
        when(col('mes').isin([6, 7, 8]), 90)  # Summer
        .when(col('mes').isin([4, 5, 9, 10]), 85)  # Spring/Fall
        .when(col('mes').isin([11, 12, 1, 2, 3]), 60)  # Winter
        .otherwise(70)
    )

def create_tourism_density_score(df, max_tourists=100000):
    """
    Create tourism density score (inverse relationship)
    """
    return df.withColumn(
        'tourism_density_score',
        100 - least(lit(100), (col('total_viajeros') / (max_tourists/100)).cast('int'))
    )

def evaluate_model_performance(predictions, label_col='visit_quality_score'):
    """
    Comprehensive model evaluation
    """
    evaluator = RegressionEvaluator(
        labelCol=label_col,
        predictionCol='prediction'
    )
    
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: 'rmse'})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: 'mae'})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
    
    return {'RMSE': rmse, 'MAE': mae, 'R2': r2}

def plot_model_results(predictions_df, model_name):
    """
    Create comprehensive plots for model evaluation
    """
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # Predictions vs Actual
    axes[0, 0].scatter(predictions_df['visit_quality_score'], predictions_df['prediction'], alpha=0.6)
    axes[0, 0].plot([predictions_df['visit_quality_score'].min(), predictions_df['visit_quality_score'].max()],
                    [predictions_df['visit_quality_score'].min(), predictions_df['visit_quality_score'].max()], 'r--')
    axes[0, 0].set_xlabel('Actual Visit Quality Score')
    axes[0, 0].set_ylabel('Predicted Visit Quality Score')
    axes[0, 0].set_title(f'{model_name}: Predictions vs Actual')
    
    # Residuals
    residuals = predictions_df['visit_quality_score'] - predictions_df['prediction']
    axes[0, 1].scatter(predictions_df['prediction'], residuals, alpha=0.6)
    axes[0, 1].axhline(y=0, color='r', linestyle='--')
    axes[0, 1].set_xlabel('Predicted Visit Quality Score')
    axes[0, 1].set_ylabel('Residuals')
    axes[0, 1].set_title(f'{model_name}: Residual Plot')
    
    # Distribution of predictions
    axes[1, 0].hist(predictions_df['visit_quality_score'], alpha=0.7, label='Actual', bins=30)
    axes[1, 0].hist(predictions_df['prediction'], alpha=0.7, label='Predicted', bins=30)
    axes[1, 0].set_xlabel('Visit Quality Score')
    axes[1, 0].set_ylabel('Frequency')
    axes[1, 0].set_title(f'{model_name}: Score Distribution')
    axes[1, 0].legend()
    
    # Time series of predictions (if date available)
    if 'fecha' in predictions_df.columns:
        predictions_df['fecha'] = pd.to_datetime(predictions_df['fecha'])
        predictions_df_sorted = predictions_df.sort_values('fecha')
        axes[1, 1].plot(predictions_df_sorted['fecha'], predictions_df_sorted['visit_quality_score'], 
                       label='Actual', alpha=0.7)
        axes[1, 1].plot(predictions_df_sorted['fecha'], predictions_df_sorted['prediction'], 
                       label='Predicted', alpha=0.7)
        axes[1, 1].set_xlabel('Date')
        axes[1, 1].set_ylabel('Visit Quality Score')
        axes[1, 1].set_title(f'{model_name}: Time Series')
        axes[1, 1].legend()
        axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.show()

def create_feature_importance_plot(feature_names, importances, model_name, top_n=15):
    """
    Create feature importance visualization
    """
    importance_df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False).head(top_n)
    
    plt.figure(figsize=(10, 8))
    sns.barplot(data=importance_df, x='importance', y='feature')
    plt.title(f'Top {top_n} Feature Importance - {model_name}')
    plt.xlabel('Importance')
    plt.tight_layout()
    plt.show()
    
    return importance_df

def create_prediction_intervals(predictions_df, confidence_level=0.95):
    """
    Create prediction intervals for uncertainty quantification
    """
    residuals = predictions_df['visit_quality_score'] - predictions_df['prediction']
    residual_std = residuals.std()
    
    # Simple approach using residual standard deviation
    z_score = 1.96 if confidence_level == 0.95 else 2.576  # for 99%
    
    predictions_df['lower_bound'] = predictions_df['prediction'] - z_score * residual_std
    predictions_df['upper_bound'] = predictions_df['prediction'] + z_score * residual_std
    
    return predictions_df

def generate_model_report(results_dict, feature_importance_df=None):
    """
    Generate a comprehensive model performance report
    """
    report = "\n" + "="*50
    report += "\n           MODEL PERFORMANCE REPORT"
    report += "\n" + "="*50 + "\n"
    
    # Model comparison
    report += "\nMODEL COMPARISON:\n"
    report += "-" * 30 + "\n"
    for model_name, metrics in results_dict.items():
        if isinstance(metrics, dict) and 'RMSE' in metrics:
            report += f"{model_name:20s} | RMSE: {metrics['RMSE']:.3f} | MAE: {metrics['MAE']:.3f} | R²: {metrics['R2']:.3f}\n"
    
    # Best model
    best_model = min(results_dict.keys(), key=lambda x: results_dict[x]['RMSE'] if isinstance(results_dict[x], dict) and 'RMSE' in results_dict[x] else float('inf'))
    report += f"\nBEST MODEL: {best_model}\n"
    report += f"Performance: RMSE={results_dict[best_model]['RMSE']:.3f}, R²={results_dict[best_model]['R2']:.3f}\n"
    
    # Feature importance
    if feature_importance_df is not None:
        report += "\nTOP 10 MOST IMPORTANT FEATURES:\n"
        report += "-" * 35 + "\n"
        for idx, row in feature_importance_df.head(10).iterrows():
            report += f"{row['feature']:25s} | {row['importance']:.4f}\n"
    
    report += "\n" + "="*50 + "\n"
    
    return report