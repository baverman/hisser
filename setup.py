from setuptools import setup, find_packages
import hisser

setup(
    name='hisser',
    version=hisser.version,
    url='https://github.com/baverman/hisser',
    author='Anton Bobrov',
    author_email='baverman@gmail.com',
    license='MIT',
    description='Fast TSDB backend for graphite',
    long_description=open('README.rst').read(),
    packages=find_packages(exclude=['tests']),
    install_requires=['msgpack', 'click', 'lmdb'],
    entry_points={
        'console_scripts': ['hisser = hisser.cli:cli']
    },
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
