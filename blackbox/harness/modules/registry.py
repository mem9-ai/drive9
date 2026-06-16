from __future__ import annotations

from .base import BaseModule
from .community_fio import CommunityFio
from .community_fsx import CommunityFSX
from .community_lock import CommunityLock
from .community_ltp import CommunityLTPFS, CommunityLTPSyscalls
from .community_mdtest import CommunityMdtest
from .community_pjdfstest import CommunityPjdfstest
from .community_pyxattr import CommunityPyxattr
from .community_vdbench import CommunityVdbench
from .drive9_workflow_auto_pack_profile import Drive9AutoPackProfile
from .drive9_workflow_auto_pack_umount_path import Drive9AutoPackUmountPath
from .drive9_workflow_git_blobless import Drive9GitBlobless
from .drive9_workflow_git_fast_clone import Drive9GitFastClone
from .drive9_workflow_git_worktree import Drive9GitWorktree
from .drive9_workflow_pack_git_clone import Drive9PackGitClone
from .drive9_workflow_pack_unpack_cli import Drive9PackUnpackCLI
from .drive9_workflow_perf import Drive9WorkflowPerf
from .drive9_workflow_portable_pack import Drive9PortablePack
from .git_official_functional import GitOfficialFunctional
from .git_official_perf import GitOfficialPerf
from .ported_juicefs_cache_consistency import PortedJuiceFSCacheConsistency
from .ported_juicefs_fsrand import PortedJuiceFSFsrand
from .ported_juicefs_random_rw import PortedJuiceFSRandomRW
from .ported_juicefs_random_stress import PortedJuiceFSRandomStress
from .ported_juicefs_rmr import PortedJuiceFSRmr


def module_registry() -> dict[str, BaseModule]:
    modules: list[BaseModule] = [
        CommunityPjdfstest(),
        CommunityLTPFS(),
        CommunityLTPSyscalls(),
        CommunityFio(),
        CommunityMdtest(),
        CommunityVdbench(),
        CommunityPyxattr(),
        CommunityFSX(),
        CommunityLock(),
        PortedJuiceFSFsrand(),
        PortedJuiceFSRandomStress(),
        PortedJuiceFSCacheConsistency(),
        PortedJuiceFSRmr(),
        PortedJuiceFSRandomRW(),
        GitOfficialFunctional(),
        GitOfficialPerf(),
        Drive9GitFastClone(),
        Drive9GitBlobless(),
        Drive9GitWorktree(),
        Drive9AutoPackProfile(),
        Drive9AutoPackUmountPath(),
        Drive9PortablePack(),
        Drive9PackUnpackCLI(),
        Drive9PackGitClone(),
        Drive9WorkflowPerf(),
    ]
    return {module.id: module for module in modules}
