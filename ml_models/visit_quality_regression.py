#!/usr/bin/env python3
"""
Visit Quality Regression Model

This script implements a regression model to predict the quality score of visiting a city on a given date.

Target Variable: Visit Quality Score (0-100)
- Weather conditions (40%)
- Tourism density (30%)
- Seasonal factors (20%)
- Hotel availability (10%)

Usage:
    python visit_quality_regression.py
"""

import sys
import os
sys.path.append('../')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Import utility functions
try:
    import utils as utils
except ImportError:
    print("Warning: Could not import exploitation.utils. Make sure the path is correct.")
    sys.exit(1)

class VisitQualityPredictor:
    """
    A comprehensive visit quality prediction model for tourism data.
    """
    
    def __init__(self):
        self.spark = None
        self.models = {}
        self.results = {}
        self.best_model = None
        self.best_model_name = None
        self.feature_names = []
        
    def initialize_spark(self):
        """Initialize Spark session with optimized configuration."""
        print("Initializing Spark session...")
        try:
            self.spark = utils.create_context()
            self.spark.conf.set('spark.sql.adaptive.enabled', 'true')
            self.spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
            print("✅ Spark session initialized successfully")
        except Exception as e:
            print(f"❌ Error initializing Spark: {e}")
            raise
    
    def load_data(self):
        """Load processed data from exploitation layer."""
        print("\n📊 Loading data from exploitation layer...")
        
        try:
            # Load weather data
            print("  Loading weather data...")
            self.df_weather = self.spark.read.format('iceberg').load('spark_catalog.exploitation.clima_barcelona')
            weather_count = self.df_weather.count()
            print(f"    Weather records: {weather_count}")
            
            # Load tourism data
            print("  Loading tourism data...")
            self.df_tourism = self.spark.read.format('iceberg').load('spark_catalog.exploitation.turismo_Provincia')
            tourism_count = self.df_tourism.count()
            print(f"    Tourism records: {tourism_count}")
            
            # Load hotel occupancy data
            print("  Loading hotel occupancy data...")
            self.df_hotels = self.spark.read.format('iceberg').load('spark_catalog.exploitation.f_ocupacion_barcelona')
            hotel_count = self.df_hotels.count()
            print(f"    Hotel records: {hotel_count}")
            
            print("✅ Data loaded successfully")
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            print("   Make sure the exploitation layer tables exist and are accessible")
            raise
    
    def create_weather_features(self, df_weather):
        """Create weather-based features."""
        print("  Creating weather features...")
        
        df_weather_features = df_weather.select(
            col('prediccion_fecha').alias('fecha'),
            col('temperatura_maxima').cast('double'),
            col('temperatura_minima').cast('double'),
            col('sens_termica_maxima').cast('double'),
            col('sens_termica_minima').cast('double'),
            col('humedad_relativa_maxima').cast('double'),
            col('humedad_relativa_minima').cast('double'),
            col('prob_precipitacion_00_24').cast('double'),
            col('uv_max').cast('double'),
            col('estado_cielo_00_24_descripcion')
        ).withColumn(
            # Create weather comfort score
            'weather_comfort_score',
            when((col('temperatura_maxima') >= 18) & (col('temperatura_maxima') <= 28) &
                 (col('prob_precipitacion_00_24') <= 30) &
                 (col('humedad_relativa_maxima') <= 70), 100)
            .when((col('temperatura_maxima') >= 15) & (col('temperatura_maxima') <= 32) &
                  (col('prob_precipitacion_00_24') <= 50), 75)
            .when((col('temperatura_maxima') >= 10) & (col('temperatura_maxima') <= 35) &
                  (col('prob_precipitacion_00_24') <= 70), 50)
            .otherwise(25)
        ).withColumn(
            # Temperature range (comfort indicator)
            'temp_range',
            col('temperatura_maxima') - col('temperatura_minima')
        )
        
        return df_weather_features
    
    def create_tourism_features(self, df_tourism):
        """Create tourism-based features."""
        print("  Creating tourism features...")
        
        df_tourism_features = df_tourism.filter(
            col('PROVINCIA_DESTINO') == 'Barcelona'
        ).groupBy(
            col('AÑO').alias('año'),
            col('MES').alias('mes')
        ).agg(
            sum('VIAJEROS').alias('total_viajeros'),
            sum('PERNOCTACIONES').alias('total_pernoctaciones'),
            avg('ESTANCIA_MEDIA').alias('avg_estancia_media')
        ).withColumn(
            # Create tourism density score (inverse relationship)
            'tourism_density_score',
            100 - least(lit(100), (col('total_viajeros') / 1000).cast('int'))
        )
        
        return df_tourism_features
    
    def create_hotel_features(self, df_hotels):
        """Create hotel-based features."""
        print("  Creating hotel features...")
        
        df_hotel_features = df_hotels.groupBy(
            col('año'),
            col('mes')
        ).agg(
            sum('viajeros').alias('hotel_viajeros'),
            sum('pernoctaciones').alias('hotel_pernoctaciones'),
            avg('estanciaMedia').alias('hotel_estancia_media'),
            avg('gradoOcupacion').alias('avg_ocupacion')
        ).withColumn(
            # Hotel availability score
            'hotel_availability_score',
            100 - col('avg_ocupacion')
        )
        
        return df_hotel_features
    
    def engineer_features(self):
        """Comprehensive feature engineering."""
        print("\n🔧 Engineering features...")
        
        # Create individual feature sets
        df_weather_feat = self.create_weather_features(self.df_weather)
        df_tourism_feat = self.create_tourism_features(self.df_tourism)
        df_hotel_feat = self.create_hotel_features(self.df_hotels)
        
        # Add temporal features to weather data
        print("  Adding temporal features...")
        df_weather_temporal = df_weather_feat.withColumn(
            'año', year(col('fecha'))
        ).withColumn(
            'mes', month(col('fecha'))
        ).withColumn(
            'dia_semana', dayofweek(col('fecha'))
        ).withColumn(
            'dia_año', dayofyear(col('fecha'))
        ).withColumn(
            'es_fin_semana', when(col('dia_semana').isin([1, 7]), 1).otherwise(0)
        ).withColumn(
            # Seasonal score
            'seasonal_score',
            when((col('mes').isin([6, 7, 8])), 90)  # Summer
            .when((col('mes').isin([4, 5, 9, 10])), 85)  # Spring/Fall
            .when((col('mes').isin([11, 12, 1, 2, 3])), 60)  # Winter
            .otherwise(70)
        )
        
        # Join all features
        print("  Joining feature sets...")
        self.df_ml = df_weather_temporal.join(
            df_tourism_feat, ['año', 'mes'], 'left'
        ).join(
            df_hotel_feat, ['año', 'mes'], 'left'
        )
        
        # Create the target variable: Visit Quality Score
        print("  Creating target variable...")
        self.df_ml = self.df_ml.withColumn(
            'visit_quality_score',
            (col('weather_comfort_score') * 0.4 +
             coalesce(col('tourism_density_score'), lit(75)) * 0.3 +
             col('seasonal_score') * 0.2 +
             coalesce(col('hotel_availability_score'), lit(75)) * 0.1)
        )
        
        print("✅ Feature engineering completed")
        
        # Show sample of engineered features
        print("\n📋 Sample of engineered features:")
        self.df_ml.select(
            'fecha', 'visit_quality_score', 'weather_comfort_score', 
            'seasonal_score', 'tourism_density_score'
        ).show(5, truncate=False)
    
    def prepare_data(self):
        """Prepare data for machine learning."""
        print("\n📋 Preparing data for ML...")
        
        # Select features for modeling
        feature_cols = [
            'temperatura_maxima', 'temperatura_minima', 'temp_range',
            'sens_termica_maxima', 'sens_termica_minima',
            'humedad_relativa_maxima', 'humedad_relativa_minima',
            'prob_precipitacion_00_24', 'uv_max',
            'mes', 'dia_semana', 'dia_año', 'es_fin_semana',
            'weather_comfort_score', 'seasonal_score'
        ]
        
        # Add tourism and hotel features if available
        optional_features = [
            'total_viajeros', 'total_pernoctaciones', 'avg_estancia_media',
            'hotel_viajeros', 'hotel_pernoctaciones', 'avg_ocupacion'
        ]
        
        # Check which optional features are available
        available_features = feature_cols.copy()
        for feat in optional_features:
            if feat in self.df_ml.columns:
                available_features.append(feat)
        
        self.feature_names = available_features
        print(f"  Using {len(available_features)} features: {available_features[:5]}...")
        
        # Prepare final dataset
        self.df_final = self.df_ml.select(
            ['fecha', 'visit_quality_score'] + available_features
        ).na.drop()
        
        final_count = self.df_final.count()
        print(f"  Final dataset: {final_count} rows, {len(self.df_final.columns)} columns")
        
        if final_count == 0:
            raise ValueError("No data remaining after cleaning. Check your data sources.")
        
        print("✅ Data preparation completed")
    
    def split_data(self):
        """Split data into training and testing sets."""
        print("\n📊 Splitting data...")
        
        # Time-based split
        self.train_data = self.df_final.filter(col('fecha') < '2023-01-01')
        self.test_data = self.df_final.filter(col('fecha') >= '2023-01-01')
        
        train_count = self.train_data.count()
        test_count = self.test_data.count()
        
        print(f"  Training data: {train_count} rows")
        print(f"  Test data: {test_count} rows")
        
        if train_count == 0 or test_count == 0:
            print("  ⚠️  Warning: One of the splits is empty. Using random split instead.")
            self.train_data, self.test_data = self.df_final.randomSplit([0.8, 0.2], seed=42)
            print(f"  Training data: {self.train_data.count()} rows")
            print(f"  Test data: {self.test_data.count()} rows")
        
        print("✅ Data split completed")
    
    def create_models(self):
        """Create ML models."""
        print("\n🤖 Creating ML models...")
        
        # Prepare features for ML
        assembler = VectorAssembler(
            inputCols=self.feature_names,
            outputCol='features'
        )
        
        scaler = StandardScaler(
            inputCol='features',
            outputCol='scaled_features'
        )
        
        # Create models
        lr = LinearRegression(
            featuresCol='scaled_features',
            labelCol='visit_quality_score',
            predictionCol='prediction'
        )
        
        rf = RandomForestRegressor(
            featuresCol='scaled_features',
            labelCol='visit_quality_score',
            predictionCol='prediction',
            numTrees=100,
            seed=42
        )
        
        gbt = GBTRegressor(
            featuresCol='scaled_features',
            labelCol='visit_quality_score',
            predictionCol='prediction',
            maxIter=100,
            seed=42
        )
        
        self.models = {
            'Linear Regression': Pipeline(stages=[assembler, scaler, lr]),
            'Random Forest': Pipeline(stages=[assembler, scaler, rf]),
            'Gradient Boosting': Pipeline(stages=[assembler, scaler, gbt])
        }
        
        print(f"  Created {len(self.models)} models: {list(self.models.keys())}")
        print("✅ Models created")
    
    def train_and_evaluate(self):
        """Train and evaluate all models."""
        print("\n🏋️ Training and evaluating models...")
        
        evaluator = RegressionEvaluator(
            labelCol='visit_quality_score',
            predictionCol='prediction'
        )
        
        for name, model in self.models.items():
            print(f"\n  Training {name}...")
            
            try:
                # Train model
                model_fitted = model.fit(self.train_data)
                
                # Make predictions
                predictions = model_fitted.transform(self.test_data)
                
                # Evaluate
                rmse = evaluator.evaluate(predictions, {evaluator.metricName: 'rmse'})
                mae = evaluator.evaluate(predictions, {evaluator.metricName: 'mae'})
                r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
                
                self.results[name] = {
                    'RMSE': rmse, 
                    'MAE': mae, 
                    'R2': r2, 
                    'model': model_fitted,
                    'predictions': predictions
                }
                
                print(f"    RMSE: {rmse:.3f}")
                print(f"    MAE: {mae:.3f}")
                print(f"    R²: {r2:.3f}")
                
            except Exception as e:
                print(f"    ❌ Error training {name}: {e}")
                continue
        
        # Find best model
        if self.results:
            self.best_model_name = min(self.results.keys(), key=lambda x: self.results[x]['RMSE'])
            self.best_model = self.results[self.best_model_name]['model']
            
            print(f"\n🏆 Best model: {self.best_model_name}")
            print(f"   RMSE: {self.results[self.best_model_name]['RMSE']:.3f}")
            print(f"   R²: {self.results[self.best_model_name]['R2']:.3f}")
        
        print("✅ Training and evaluation completed")
    
    def analyze_results(self):
        """Analyze and visualize results."""
        print("\n📈 Analyzing results...")
        
        if not self.results:
            print("❌ No results to analyze")
            return
        
        # Feature importance analysis (for tree-based models)
        if self.best_model_name in ['Random Forest', 'Gradient Boosting']:
            print("\n🔍 Feature Importance Analysis:")
            try:
                feature_importance = self.best_model.stages[-1].featureImportances.toArray()
                
                importance_df = pd.DataFrame({
                    'feature': self.feature_names,
                    'importance': feature_importance
                }).sort_values('importance', ascending=False)
                
                print("\nTop 10 Most Important Features:")
                print(importance_df.head(10).to_string(index=False))
                
                # Save feature importance
                importance_df.to_csv('ml_models/feature_importance.csv', index=False)
                print("\n💾 Feature importance saved to feature_importance.csv")
                
            except Exception as e:
                print(f"❌ Error analyzing feature importance: {e}")
        
        # Prediction analysis
        try:
            best_predictions = self.results[self.best_model_name]['predictions']
            
            # Convert to Pandas for analysis
            pred_df = best_predictions.select(
                'fecha', 'visit_quality_score', 'prediction'
            ).toPandas()
            
            print(f"\n📊 Prediction Statistics:")
            print(f"   Mean Actual Score: {pred_df['visit_quality_score'].mean():.2f}")
            print(f"   Mean Predicted Score: {pred_df['prediction'].mean():.2f}")
            print(f"   Std Actual Score: {pred_df['visit_quality_score'].std():.2f}")
            print(f"   Std Predicted Score: {pred_df['prediction'].std():.2f}")
            
            # Save predictions
            pred_df.to_csv('ml_models/predictions.csv', index=False)
            print("\n💾 Predictions saved to predictions.csv")
            
        except Exception as e:
            print(f"❌ Error analyzing predictions: {e}")
        
        print("✅ Results analysis completed")
    
    def save_model(self):
        """Save the best model and results."""
        print("\n💾 Saving model and results...")
        
        if not self.best_model:
            print("❌ No model to save")
            return
        
        try:
            # Save the best model
            model_path = 'ml_models/best_visit_quality_model'
            self.best_model.write().overwrite().save(model_path)
            print(f"   Best model ({self.best_model_name}) saved to {model_path}")
            
            # Save results summary
            results_summary = pd.DataFrame({
                name: {k: v for k, v in metrics.items() if k != 'model' and k != 'predictions'}
                for name, metrics in self.results.items()
            }).T
            
            results_summary.to_csv('ml_models/model_comparison_results.csv')
            print("   Model comparison results saved to model_comparison_results.csv")
            
            print("✅ Model and results saved successfully")
            
        except Exception as e:
            print(f"❌ Error saving model: {e}")
    
    def generate_report(self):
        """Generate a comprehensive report."""
        print("\n📋 Generating comprehensive report...")
        
        report = []
        report.append("=" * 60)
        report.append("           VISIT QUALITY PREDICTION MODEL REPORT")
        report.append("=" * 60)
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Data summary
        report.append("DATA SUMMARY:")
        report.append("-" * 20)
        if hasattr(self, 'df_final'):
            report.append(f"Total records: {self.df_final.count()}")
            report.append(f"Features used: {len(self.feature_names)}")
            report.append(f"Training records: {self.train_data.count()}")
            report.append(f"Test records: {self.test_data.count()}")
        report.append("")
        
        # Model comparison
        if self.results:
            report.append("MODEL COMPARISON:")
            report.append("-" * 30)
            for model_name, metrics in self.results.items():
                report.append(f"{model_name:20s} | RMSE: {metrics['RMSE']:.3f} | MAE: {metrics['MAE']:.3f} | R²: {metrics['R2']:.3f}")
            
            report.append("")
            report.append(f"BEST MODEL: {self.best_model_name}")
            report.append(f"Performance: RMSE={self.results[self.best_model_name]['RMSE']:.3f}, R²={self.results[self.best_model_name]['R2']:.3f}")
        
        report.append("")
        report.append("=" * 60)
        
        report_text = "\n".join(report)
        print(report_text)
        
        # Save report
        with open('ml_models/model_report.txt', 'w') as f:
            f.write(report_text)
        
        print("\n💾 Report saved to model_report.txt")
    
    def cleanup(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
            print("\n🧹 Spark session stopped")
    
    def run_full_pipeline(self):
        """Run the complete ML pipeline."""
        print("🚀 Starting Visit Quality Prediction Model Pipeline")
        print("=" * 60)
        
        try:
            self.initialize_spark()
            self.load_data()
            self.engineer_features()
            self.prepare_data()
            self.split_data()
            self.create_models()
            self.train_and_evaluate()
            self.analyze_results()
            self.save_model()
            self.generate_report()
            
            print("\n🎉 Pipeline completed successfully!")
            print("\nFiles generated:")
            print("  - ml_models/best_visit_quality_model/ (trained model)")
            print("  - ml_models/model_comparison_results.csv")
            print("  - ml_models/feature_importance.csv")
            print("  - ml_models/predictions.csv")
            print("  - ml_models/model_report.txt")
            
        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.cleanup()

def main():
    """Main function to run the visit quality prediction model."""
    predictor = VisitQualityPredictor()
    predictor.run_full_pipeline()

if __name__ == "__main__":
    main()