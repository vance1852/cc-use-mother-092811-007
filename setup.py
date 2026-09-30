from setuptools import find_packages, setup

setup(
    name="transport-coordination",
    version="0.1.0",
    description="国际通道韧性编排协同服务",
    package_dir={"": "src"},
    packages=find_packages("src"),
    python_requires=">=3.11",
)
