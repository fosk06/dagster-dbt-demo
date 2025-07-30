from sqlmesh.core.console import TerminalConsole
from sqlmesh.core.model.definition import AuditResult
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.core.snapshot.definition import Interval
from typing import List, Dict, Any, Optional
from dagster import get_dagster_logger
import logging

class SQLMeshDagsterConsole(TerminalConsole):
    """Console custom qui capture les résultats d'audit de manière précise"""
    
    def __init__(self, logger=None, **kwargs):
        super().__init__(**kwargs)
        self.audit_results: List[Dict[str, Any]] = []
        self.audit_stats: Dict[str, Dict[str, int]] = {}  # Par snapshot
        # Log unique pour signaler l'activation de la console custom
        print("🚀 SQLMeshDagsterConsole custom activée")
    
    def update_snapshot_evaluation_progress(
        self,
        snapshot: Snapshot,
        interval: Interval,
        batch_idx: int,
        duration_ms: Optional[int],
        num_audits_passed: int,
        num_audits_failed: int,
        audit_only: bool = False,
    ) -> None:
        """Capture les statistiques d'audit précises"""
        snapshot_name = snapshot.name
        
        # Log pour débugger
        print(f"🔍 AUDIT CAPTURE - Snapshot: {snapshot_name}")
        print(f"   📊 Passed: {num_audits_passed}, Failed: {num_audits_failed}")
        print(f"   📅 Interval: {interval}")
        print(f"   🔄 Batch: {batch_idx}, Duration: {duration_ms}ms")
        print(f"   🎯 Audit only: {audit_only}")
        
        # Stocker les statistiques d'audit pour ce snapshot
        if snapshot_name not in self.audit_stats:
            self.audit_stats[snapshot_name] = {
                'passed': 0,
                'failed': 0,
                'total': 0
            }
            print(f"   🆕 Nouveau snapshot ajouté: {snapshot_name}")
        
        # Mettre à jour les stats
        old_passed = self.audit_stats[snapshot_name]['passed']
        old_failed = self.audit_stats[snapshot_name]['failed']
        
        self.audit_stats[snapshot_name]['passed'] += num_audits_passed
        self.audit_stats[snapshot_name]['failed'] += num_audits_failed
        self.audit_stats[snapshot_name]['total'] += (num_audits_passed + num_audits_failed)
        
        # Log des changements
        if num_audits_passed > 0 or num_audits_failed > 0:
            print(f"   📈 Stats mises à jour pour {snapshot_name}:")
            print(f"      Avant: {old_passed} passed, {old_failed} failed")
            print(f"      Après: {self.audit_stats[snapshot_name]['passed']} passed, {self.audit_stats[snapshot_name]['failed']} failed")
        
        # Appeler la méthode parent pour l'affichage normal
        super().update_snapshot_evaluation_progress(
            snapshot, interval, batch_idx, duration_ms, 
            num_audits_passed, num_audits_failed, audit_only
        )
    
    def log_audit_result(self, audit_result: AuditResult) -> None:
        """Capture les résultats d'audit structurés"""
        print(f"🔍 AUDIT RESULT CAPTURE - {audit_result.name}")
        print(f"   📋 Model: {audit_result.model}")
        print(f"   ✅ Passed: {audit_result.passed}")
        print(f"   💬 Message: {audit_result.message}")
        
        audit_data = {
            "name": audit_result.name,
            "model": audit_result.model,
            "passed": audit_result.passed,
            "message": audit_result.message,
            "details": getattr(audit_result, 'details', None)
        }
        self.audit_results.append(audit_data)
        
        print(f"   💾 Audit result ajouté (total: {len(self.audit_results)})")
        
        # Appeler la méthode parent
        super().log_audit_result(audit_result)
    
    # Méthodes utilitaires pour récupérer les données capturées
    def get_audit_stats(self) -> Dict[str, Dict[str, int]]:
        """Récupère les statistiques d'audit par snapshot"""
        print(f"📊 Récupération des stats d'audit: {len(self.audit_stats)} snapshots")
        return self.audit_stats.copy()
    
    def get_audit_results(self) -> List[Dict[str, Any]]:
        """Récupère les résultats d'audit structurés"""
        print(f"📋 Récupération des résultats d'audit: {len(self.audit_results)} résultats")
        return self.audit_results.copy()
    
    def clear_captured_data(self) -> None:
        """Efface toutes les données capturées"""
        print("🧹 Nettoyage des données capturées")
        self.audit_results.clear()
        self.audit_stats.clear()
    
    def get_audit_summary(self) -> Dict[str, Any]:
        """Retourne un résumé complet des audits"""
        total_passed = sum(stats['passed'] for stats in self.audit_stats.values())
        total_failed = sum(stats['failed'] for stats in self.audit_stats.values())
        total_audits = sum(stats['total'] for stats in self.audit_stats.values())
        
        summary = {
            "total_audits": total_audits,
            "total_passed": total_passed,
            "total_failed": total_failed,
            "success_rate": (total_passed / total_audits * 100) if total_audits > 0 else 0,
            "by_snapshot": self.audit_stats.copy(),
            "structured_results": self.audit_results.copy()
        }
        
        print(f"📈 Résumé d'audit: {total_audits} total, {total_passed} réussis, {total_failed} échoués")
        return summary