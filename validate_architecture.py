#!/usr/bin/env python3
"""
Script para validar la arquitectura TravelMind
Verifica que todos los servicios estén funcionando correctamente
"""

import requests
import time
import sys
from typing import Dict, List, Tuple

def check_service(name: str, url: str, timeout: int = 5) -> Tuple[bool, str]:
    """
    Verifica si un servicio está disponible
    """
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code == 200:
            return True, f"✅ {name} está funcionando correctamente"
        else:
            return False, f"❌ {name} respondió con código {response.status_code}"
    except requests.exceptions.ConnectionError:
        return False, f"❌ {name} no está disponible (conexión rechazada)"
    except requests.exceptions.Timeout:
        return False, f"❌ {name} no responde (timeout)"
    except Exception as e:
        return False, f"❌ {name} error: {str(e)}"

def validate_architecture() -> Dict[str, bool]:
    """
    Valida todos los servicios de la arquitectura
    """
    services = {
        "MinIO Console": "http://localhost:9001",
        "MinIO API": "http://localhost:9000/minio/health/live",
        "Spark Master UI": "http://localhost:8080",
        "Airflow Web UI": "http://localhost:8081",
        "PostgreSQL": "http://localhost:5432"  # Este fallará pero lo incluimos para completitud
    }
    
    results = {}
    print("🔍 Validando arquitectura TravelMind...\n")
    
    for service_name, url in services.items():
        if service_name == "PostgreSQL":
            # PostgreSQL no tiene endpoint HTTP, lo marcamos como pendiente
            results[service_name] = False
            print(f"⏳ {service_name} - Verificación manual requerida (puerto 5432)")
            continue
            
        success, message = check_service(service_name, url)
        results[service_name] = success
        print(message)
        time.sleep(1)  # Pequeña pausa entre verificaciones
    
    return results

def check_docker_containers():
    """
    Verifica que los contenedores estén ejecutándose
    """
    import subprocess
    
    print("\n🐳 Verificando contenedores Docker...\n")
    
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al verificar contenedores: {e}")
        return False
    except FileNotFoundError:
        print("❌ Docker no está instalado o no está en el PATH")
        return False

def main():
    """
    Función principal de validación
    """
    print("=" * 60)
    print("🚀 VALIDACIÓN DE ARQUITECTURA TRAVELMIND")
    print("=" * 60)
    
    # Verificar contenedores Docker
    containers_ok = check_docker_containers()
    
    if not containers_ok:
        print("\n❌ No se pueden verificar los contenedores Docker")
        sys.exit(1)
    
    # Dar tiempo a que los servicios se inicialicen
    print("\n⏳ Esperando a que los servicios se inicialicen...")
    time.sleep(10)
    
    # Validar servicios
    results = validate_architecture()
    
    # Resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN DE VALIDACIÓN")
    print("=" * 60)
    
    total_services = len(results)
    successful_services = sum(1 for success in results.values() if success)
    
    print(f"Servicios verificados: {total_services}")
    print(f"Servicios funcionando: {successful_services}")
    print(f"Tasa de éxito: {(successful_services/total_services)*100:.1f}%")
    
    if successful_services == total_services:
        print("\n🎉 ¡Todos los servicios están funcionando correctamente!")
        return 0
    else:
        print(f"\n⚠️  {total_services - successful_services} servicio(s) requieren atención")
        return 1

if __name__ == "__main__":
    sys.exit(main())
