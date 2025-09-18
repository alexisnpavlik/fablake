"""
Tests básicos para la librería spark_data_lib.
Demuestra cómo testear funciones de ingeniería de datos.
"""

import pytest
from pyspark.sql import SparkSession
from fablake.utils import create_spark_session
from spark_data_lib.transformers import remove_nulls, remove_duplicates, clean_text_columns
from spark_data_lib.validators import validate_nulls, validate_duplicates


@pytest.fixture(scope="session")
def spark():
    """Fixture para crear una sesión de Spark para testing."""
    spark = create_spark_session(app_name="TestSparkDataLib")
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    """Fixture con datos de prueba."""
    data = [
        ("Juan", "Pérez", 25, "juan@email.com"),
        ("María", None, 30, "maria@email.com"),
        (" Pedro ", "López", None, "pedro@email.com"),
        ("Juan", "Pérez", 25, "juan@email.com"),  # Duplicado
        (None, "García", 28, None),
    ]
    columns = ["nombre", "apellido", "edad", "email"]
    return spark.createDataFrame(data, columns)


def test_remove_nulls(sample_data):
    """Test para la función remove_nulls."""
    # Remover filas con cualquier valor nulo
    result = remove_nulls(sample_data, how="any")
    
    # Debe quedar solo 1 fila (la duplicada se cuenta)
    assert result.count() == 2
    
    # Remover filas solo si todas las columnas son nulas
    result_all = remove_nulls(sample_data, how="all")
    
    # No debería remover ninguna fila ya que ninguna es completamente nula
    assert result_all.count() == 5


def test_remove_duplicates(sample_data):
    """Test para la función remove_duplicates."""
    result = remove_duplicates(sample_data)
    
    # Debe remover 1 fila duplicada
    assert result.count() == 4


def test_clean_text_columns(sample_data):
    """Test para la función clean_text_columns."""
    result = clean_text_columns(
        sample_data, 
        columns=["nombre"], 
        operations=["trim", "lower"]
    )
    
    # Verificar que " Pedro " se convirtió en "pedro"
    pedro_row = result.filter(result.nombre == "pedro").collect()
    assert len(pedro_row) == 1


def test_validate_nulls(sample_data):
    """Test para la función validate_nulls."""
    # Validar con umbral bajo (debería fallar)
    result = validate_nulls(sample_data, threshold=10.0)
    assert not result["passed"]
    
    # Validar con umbral alto (debería pasar)
    result_high = validate_nulls(sample_data, threshold=50.0)
    assert result_high["passed"]


def test_validate_duplicates(sample_data):
    """Test para la función validate_duplicates."""
    # Validar con umbral bajo (debería fallar por duplicados)
    result = validate_duplicates(sample_data, threshold=10.0)
    assert not result["passed"]
    
    # El porcentaje de duplicados debería ser 20% (1 de 5)
    assert result["duplicate_percentage"] == 20.0


def test_data_processing_pipeline(spark):
    """Test de un pipeline completo de procesamiento."""
    # Crear datos de prueba
    data = [
        ("producto_a", 100, 5),
        ("producto_b", 200, None),
        ("producto_a", 100, 5),  # Duplicado
        (" producto_c ", 300, 2),
    ]
    columns = ["producto", "precio", "cantidad"]
    df = spark.createDataFrame(data, columns)
    
    # Pipeline de limpieza
    df_cleaned = clean_text_columns(df, ["producto"], ["trim", "lower"])
    df_no_duplicates = remove_duplicates(df_cleaned)
    df_filled = df_no_duplicates.fillna({"cantidad": 0})
    
    # Verificaciones
    assert df_filled.count() == 3  # Removió 1 duplicado
    
    # Verificar que no hay nulos en cantidad
    null_count = df_filled.filter(df_filled.cantidad.isNull()).count()
    assert null_count == 0
    
    # Verificar que producto_c fue limpiado
    producto_c = df_filled.filter(df_filled.producto == "producto_c").collect()
    assert len(producto_c) == 1


if __name__ == "__main__":
    # Ejecutar tests básicos sin pytest
    print("Ejecutando tests básicos...")
    
    spark = create_spark_session(app_name="BasicTests")
    
    try:
        # Crear datos de prueba
        data = [
            ("Juan", "Pérez", 25),
            ("María", None, 30),
            ("Juan", "Pérez", 25),  # Duplicado
        ]
        columns = ["nombre", "apellido", "edad"]
        df = spark.createDataFrame(data, columns)
        
        print("Datos originales:")
        df.show()
        
        # Test remove_duplicates
        df_no_dup = remove_duplicates(df)
        print(f"Sin duplicados: {df_no_dup.count()} filas")
        
        # Test remove_nulls
        df_no_null = remove_nulls(df, how="any")
        print(f"Sin nulos: {df_no_null.count()} filas")
        
        # Test validaciones
        null_validation = validate_nulls(df, threshold=30.0)
        print(f"Validación nulos pasó: {null_validation['passed']}")
        
        dup_validation = validate_duplicates(df, threshold=30.0)
        print(f"Validación duplicados pasó: {dup_validation['passed']}")
        
        print("Tests básicos completados exitosamente!")
        
    finally:
        spark.stop()
