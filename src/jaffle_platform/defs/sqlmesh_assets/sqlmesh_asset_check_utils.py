# Utility functions for SQLMesh AssetCheckSpec creation

from dagster import AssetCheckSpec, AssetKey
from typing import List
from sqlmesh.core.model.definition import ExternalModel


def create_asset_checks_from_model(model, asset_key: AssetKey) -> List[AssetCheckSpec]:
    """
    Crée les AssetCheckSpec pour les audits d'un modèle SQLMesh.
    
    Args:
        model: Modèle SQLMesh
        asset_key: AssetKey Dagster associé au modèle
    
    Returns:
        Liste des AssetCheckSpec pour les audits du modèle
    """
    asset_checks = []
    
    # Récupérer les audits du modèle
    audits_with_args = model.audits_with_args if hasattr(model, 'audits_with_args') else []
    
    for audit_obj, audit_args in audits_with_args:
        asset_checks.append(
            AssetCheckSpec(
                name=audit_obj.name,
                asset=asset_key,  # ← C'est "asset" pas "asset_key" !
                description=f"Triggered by sqlmesh audit {audit_obj.name} on model {model.name}",
                blocking=False,  # ← sqlmesh can block materialization if audit fails, but we don't want to block dagster
                metadata={
                    "audit_query": str(audit_obj.query.sql()),
                    "audit_blocking": audit_obj.blocking,  # ← Garder l'info originale dans les métadonnées
                    "audit_dialect": audit_obj.dialect,
                    "audit_args": audit_args
                }
            )
        )
    
    return asset_checks


def create_all_asset_checks(models, translator) -> List[AssetCheckSpec]:
    """
    Crée tous les AssetCheckSpec pour tous les modèles SQLMesh.
    
    Args:
        models: Liste des modèles SQLMesh
        translator: SQLMeshTranslator pour mapper les modèles vers AssetKey
    
    Returns:
        Liste de tous les AssetCheckSpec
    """
    all_checks = []
    
    for model in models:
        # Ignorer les external models
        if isinstance(model, ExternalModel):
            continue
            
        asset_key = translator.get_asset_key(model)
        model_checks = create_asset_checks_from_model(model, asset_key)
        all_checks.extend(model_checks)
    
    return all_checks 