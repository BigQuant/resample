"""resample package.

resample
"""

from bigmodule import I

# 需要安装的第三方依赖包
# from bigmodule import R
# R.require("requests>=2.0", "isort==5.13.2")

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据处理"
# 模块显示名
friendly_name = "数据重采样resample"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/"
# 是否自动缓存结果
cacheable = True


def run(
    input_data: I.port('数据输入', specific_type_name = 'DataSource'),  # type: ignore
    group: I.str('分组列') = 'instrument',  # type: ignore
    sessions: I.str('频率类别') = 'Q'  # type: ignore
)->[
    I.port("重采样后的输出数据", "data")  # type: ignore
]:
    import dai
    
    df = input_data.read()
    df = df.groupby(group).apply(lambda tmp:tmp.set_index('date').resample(sessions).ffill().reset_index()).reset_index(drop=True)
    res = dai.DataSource.write_bdb(df)
    return I.Outputs(data=res)


def post_run(outputs):
    """后置运行函数"""
    return outputs
