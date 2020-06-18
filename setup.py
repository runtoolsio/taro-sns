import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="taro-sns",
    version="0.0.3",
    author="Stan Svec",
    author_email="stan.x.svec@gmail.com",
    description="Plugin allowing to send SNS messages on job state changes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/StanSvec/taro_sns",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers, Ops, Admins",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.6',
    packages=setuptools.find_packages(include=("taro_sns",), exclude=("test",)),
    install_requires=[
        "boto3>=1.12.39",
        "yaql>=1.1.3",
    ],
    package_data={
        'taro_sns': ['config/*.yaml'],
    },
)
