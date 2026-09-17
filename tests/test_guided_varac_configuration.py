import pytest

from freqinout.core.guided_radio_software_model import RadioRole
from freqinout.core.guided_varac_configuration import (
    GuidedVarACConfigurationError, VarACClusterIdentity, VarACClusterMembership,
    VarACClusterPath, VarACConfigurationRequest, VarACNodeIdentity,
    VarACPlanningInventory, plan_varac_configuration,
)


def _node(key="north", radio="radio-north", root="/varac/north", *, command="wine VarAC.exe"):
    return VarACNodeIdentity(
        node_key=key, radio_key=radio, install_path=f"{root}/install",
        launch_command=command, working_directory=f"{root}/work",
        ini_path=f"{root}/VarAC.ini", database_path=f"{root}/VarAC.db",
        incoming_path=f"{root}/incoming", outbox_path=f"{root}/outbox",
    )


def test_standalone_plan_preserves_exact_node_command_and_has_no_membership():
    node = _node(command='wine start /unix "/opt/VarAC.exe" --profile N')
    plan = plan_varac_configuration(
        VarACConfigurationRequest("standalone", RadioRole.TRANSCEIVER, node, VarACClusterPath.STANDALONE),
        VarACPlanningInventory(),
    )
    assert plan.node.launch_command == 'wine start /unix "/opt/VarAC.exe" --profile N'
    assert plan.node.working_directory == "/varac/north/work"
    assert plan.cluster is plan.membership is None
    assert {claim.kind for claim in plan.resource_claims} >= {"node-local-database-path", "launch-identity"}


def test_create_cluster_is_one_plan_with_enabled_first_member_then_gateway():
    node = _node()
    cluster = VarACClusterIdentity("Front Range", "/varac/shared/cluster.db", counter_refresh_seconds=15, ptt_lock_enabled=True)
    plan = plan_varac_configuration(
        VarACConfigurationRequest("create", "tx_rx", node, "create_cluster", cluster=cluster, instance_number=1, gateway_for_new_cluster=True),
        VarACPlanningInventory(),
    )
    assert plan.cluster is cluster
    assert plan.membership is not None and plan.membership.enabled and plan.membership.instance_number == 1
    assert plan.gateway_node_key == node.node_key
    shared = [claim for claim in plan.resource_claims if claim.kind == "cluster-shared-database"]
    assert len(shared) == 1 and shared[0].exclusive is False


def test_cluster_id_collision_is_case_insensitive_and_does_not_mutate_inventory():
    inventory = VarACPlanningInventory(clusters=(VarACClusterIdentity("Front Range"),))
    request = VarACConfigurationRequest("create", "tx_rx", _node(), "create_cluster", cluster=VarACClusterIdentity("  FRONT   range  "), instance_number=1)
    with pytest.raises(GuidedVarACConfigurationError, match="already in use"):
        plan_varac_configuration(request, inventory)
    assert inventory.clusters[0].public_id == "Front Range"


def test_join_cluster_requires_positive_distinct_enabled_member_number():
    cluster = VarACClusterIdentity("Net A", "/varac/shared/a.db")
    inventory = VarACPlanningInventory(
        nodes=(_node("south", "radio-south", "/varac/south"),), clusters=(cluster,),
        memberships=(VarACClusterMembership("Net A", "south", "radio-south", 2, enabled=True),),
    )
    duplicate = VarACConfigurationRequest("join", "transceiver", _node(), "join_cluster", join_cluster_id="NET a", instance_number=2)
    with pytest.raises(GuidedVarACConfigurationError, match="instance 2 is already assigned"):
        plan_varac_configuration(duplicate, inventory)
    plan = plan_varac_configuration(
        VarACConfigurationRequest("join", "transceiver", _node(), "join_cluster", join_cluster_id="NET a", instance_number=3), inventory,
    )
    assert plan.membership is not None and plan.membership.cluster_id == "net a"


def test_disabled_existing_member_does_not_reserve_its_instance_number():
    cluster = VarACClusterIdentity("Net A")
    inventory = VarACPlanningInventory(
        clusters=(cluster,), memberships=(VarACClusterMembership("Net A", "old", "old-radio", 7, enabled=False),),
    )
    plan = plan_varac_configuration(
        VarACConfigurationRequest("join", "tx_rx", _node(), "join_cluster", join_cluster_id="net a", instance_number=7), inventory,
    )
    assert plan.membership is not None and plan.membership.instance_number == 7


@pytest.mark.parametrize("path", ["create_cluster", "join_cluster", "standalone"])
def test_observer_is_rejected_fail_closed_before_any_plan(path):
    kwargs = {}
    if path == "create_cluster":
        kwargs.update(cluster=VarACClusterIdentity("Net A"), instance_number=1)
    elif path == "join_cluster":
        kwargs.update(join_cluster_id="Net A", instance_number=1)
    inventory = VarACPlanningInventory(clusters=(VarACClusterIdentity("Net A"),))
    request = VarACConfigurationRequest("observer", RadioRole.OBSERVER, _node(), path, **kwargs)
    with pytest.raises(GuidedVarACConfigurationError, match="not available to observer"):
        plan_varac_configuration(request, inventory)


def test_shared_database_can_never_be_substituted_for_node_local_database():
    node = _node()
    request = VarACConfigurationRequest(
        "unsafe", "transceiver", node, "create_cluster",
        cluster=VarACClusterIdentity("Net A", node.database_path), instance_number=1,
    )
    with pytest.raises(GuidedVarACConfigurationError, match="cannot replace a node-local path"):
        plan_varac_configuration(request, VarACPlanningInventory())


def test_node_path_and_launch_collisions_fail_before_a_plan_is_returned():
    existing = _node("existing", "radio-existing", "/varac/existing")
    inventory = VarACPlanningInventory(nodes=(existing,))
    same_db = _node("new", "radio-new", "/varac/new")
    same_db = VarACNodeIdentity(
        same_db.node_key, same_db.radio_key, same_db.install_path, same_db.launch_command,
        same_db.working_directory, same_db.ini_path, existing.database_path, same_db.incoming_path, same_db.outbox_path,
    )
    with pytest.raises(GuidedVarACConfigurationError, match="node-local path"):
        plan_varac_configuration(VarACConfigurationRequest("collision", "tx_rx", same_db, "standalone"), inventory)
    same_launch = _node("new", "radio-new", "/varac/new", command=existing.launch_command)
    same_launch = VarACNodeIdentity(
        same_launch.node_key, same_launch.radio_key, same_launch.install_path, same_launch.launch_command,
        existing.working_directory, same_launch.ini_path, same_launch.database_path, same_launch.incoming_path, same_launch.outbox_path,
    )
    with pytest.raises(GuidedVarACConfigurationError, match="launch identity"):
        plan_varac_configuration(VarACConfigurationRequest("collision", "tx_rx", same_launch, "standalone"), inventory)


def test_replacement_is_atomic_and_cannot_change_radio_owner():
    old = _node("north", "radio-north", "/varac/old")
    inventory = VarACPlanningInventory(nodes=(old,))
    replacement = _node("north-replaced", "radio-north", "/varac/new")
    plan = plan_varac_configuration(
        VarACConfigurationRequest("replace", "tx_rx", replacement, "standalone", replace_node_key="north"), inventory,
    )
    assert plan.node.node_key == "north-replaced"
    with pytest.raises(GuidedVarACConfigurationError, match="retain the owning radio"):
        plan_varac_configuration(
            VarACConfigurationRequest("replace", "tx_rx", _node("new", "other-radio", "/varac/new"), "standalone", replace_node_key="north"), inventory,
        )
