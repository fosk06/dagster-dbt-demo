import os
from datacontract.data_contract import DataContract

def test_data_contract(model_name: str):
    """
    Teste un data contract mono-modèle à partir de son nom (ex: 'raw_users').
    Retourne un dict {success: bool, errors: [str]}
    """
    base_path = os.path.dirname(__file__)
    yaml_path = os.path.join(base_path, f"{model_name}.yaml")
    if not os.path.exists(yaml_path):
        return {"success": False, "errors": [f"Fichier YAML non trouvé: {yaml_path}"]}
    dc = DataContract(data_contract_file=yaml_path)
    run = dc.test()
    if run.has_passed():
        return {"success": True, "errors": []}
    # Récupère les erreurs des checks
    errors = [
        f"{check.name or check.id}: {check.result} - {check.reason or check.details}"
        for check in run.checks if check.result in ("failed", "error")
    ]
    # Ajoute les logs d'erreur
    errors += [
        f"[ERROR] {log.timestamp}: {log.message}"
        for log in run.logs if log.level == "ERROR"
    ]
    return {"success": False, "errors": errors, "run": run} 