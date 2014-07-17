class ActionError(Exception):
    def __init__(self, message, action, description=None, properties={}):
        Exception.__init__(self, message)
        self.action = action
        self.descripton = description
        self.properties=properties

def raiseActionError(message, action, description=None):
    raise ActionError(message, action, description)
