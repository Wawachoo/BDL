from setuptools import setup


setup(name='bdl',
      version='1.4.0',
      description='A download framework',
      url='https://www.github.com/Wawachoo/BDL',
      author='Wawachoo',
      author_email='Wawachoo@users.noreply.github.com',
      license='GPLv3',
      classifiers=['Development Status :: 3 - Alpha',
                   'Environment :: Console',
                   'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
                   'Natural Language :: English',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python :: 3 :: Only',
                   'Communications :: File Sharing',
                   'Topic :: Internet'],
      keywords='download framework',
      packages=['bdl', ],
      install_requires=['requests', ],
      scripts=["scripts/bdl", ])
