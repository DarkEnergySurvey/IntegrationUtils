import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*")

# The main call
setup(name='IntegrationUtils',
      version ='2.0.9',
      license = "GPL",
      description = "DESDM's integration utils",
      author = "Michelle Gower",
      author_email = "mgower@illinois.edu",
      packages = ['intgutils'],
      package_dir = {'': 'python'},
      scripts = bin_files,
      data_files=[('ups',['ups/IntegrationUtils.table'])]
      )

