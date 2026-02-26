from .. import domaindefinition as shdomaindef

def get_step_definition_name(step_definition_type: type[shdomaindef.StepDefinition]):
    if not issubclass(step_definition_type, shdomaindef.StepDefinition):
        raise TypeError("step_definition_type must be a subclass of StepDefinition")
    return step_definition_type.__name__.lower()
