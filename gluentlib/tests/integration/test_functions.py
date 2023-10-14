from gluentlib.config.orchestration_config import OrchestrationConfig


def build_current_options():
    return OrchestrationConfig.from_dict({'verbose': False,
                                          'execute': False})
