'''
Created on Oct 15, 2012

@author: dballest
'''

class parameterStorage(object):
    
    def __init__(self, f):
        """
        __init__
        
        Stores the function and valid parameters to save/restore.
        
        Supported parameters are:
        
        Global tag (self.globalTag)
        CMSSW version (self.frameworkVersion)
        Scram arch (self.scramArch)
        
        Primary Dataset (self.inputPrimaryDataset)
        Processing Version (self.processingVersion)
        Processing String (self.processingString)
        Acquisition Era (self.acquisitionEra)
        """
        print "Decorator init"
        self.f = f
        self.resetParameters()
        
        return
        
        
    def __get__(self, instance, owner):
        print "Getting the decorator"
        self.cls = owner
        self.obj = instance
        return self.__call__

    def __call__(self, *args, **kwargs):
        """
        __call__
        
        Actually does the wrapping, calls the stored function
        with the given arguments but first changes self
        """
        print 'instance %s of class %s this is now decorated whee!' % (
            self.obj, self.cls
        )
        self.obj.x = 8
        print 'Lawyered'
        self.f(self.obj, *args, **kwargs)
        return
        
        
    def resetParameters(self):
        """
        _resetParameters_
        
        Reset parameters to None
        """
        self.globalTag = None
        self.frameworkVersion = None
        self.scramArch = None
        self.inputPrimaryDataset = None
        self.processingVersion = None
        self.processingString = None
        self.acquisitionEra = None
        
        return

class myClass():
    
    def __init__(self):
        print "MyClass init"
        self.x = 0

    @parameterStorage
    def decorated(self, m):
        self.x = m
        return
    
if __name__ == "__main__":
    print "Initializing"
    x = myClass()
    print "Instanced myClass"
    x.decorated(6)
    print "Called Decorated method"
    print x.x