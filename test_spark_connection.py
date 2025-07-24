#!/usr/bin/env python3
"""
Script para probar la conectividad con Spark desde fuera del contenedor
"""

import requests
import json
from typing import Dict, Any

def test_spark_master() -> Dict[str, Any]:
    """
    Prueba la conectividad con Spark Master
    """
    try:
        # Verificar Spark Master UI
        response = requests.get("http://localhost:8080/json/", timeout=10)
        if response.status_code == 200:
            data = response.json()
            return {
                "status": "success",
                "master_url": data.get("url", "N/A"),
                "workers": len(data.get("workers", [])),
                "cores": data.get("cores", 0),
                "memory": data.get("memory", "0 MB"),
                "alive_workers": len([w for w in data.get("workers", []) if w.get("state") == "ALIVE"])
            }
        else:
            return {"status": "error", "message": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def main():
    """
    Función principal
    """
    print("🔥 Probando conectividad con Spark...")
    print("=" * 50)
    
    result = test_spark_master()
    
    if result["status"] == "success":
        print("✅ Spark Master está funcionando!")
        print(f"📍 Master URL: {result['master_url']}")
        print(f"👥 Workers totales: {result['workers']}")
        print(f"💚 Workers activos: {result['alive_workers']}")
        print(f"🔧 Cores disponibles: {result['cores']}")
        print(f"💾 Memoria disponible: {result['memory']}")
        
        if result['alive_workers'] > 0:
            print("\n🎉 ¡Cluster Spark listo para procesar trabajos!")
        else:
            print("\n⚠️  No hay workers activos. Verifica la configuración.")
    else:
        print(f"❌ Error: {result['message']}")
        print("\n💡 Sugerencias:")
        print("   - Verifica que docker-compose esté ejecutándose")
        print("   - Espera unos minutos más para que Spark se inicialice")
        print("   - Revisa los logs: docker-compose logs spark-master")

if __name__ == "__main__":
    main()
