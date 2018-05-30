from setuptools import setup, find_packages

setup(name='fixc',
      version='0.1',
      author='Fofo',
      author_email='willpowerli@163.com',
      license='MIT',
      packages=find_packages(),
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)
