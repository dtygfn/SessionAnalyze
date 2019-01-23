# SessionAnalyze
一个用户点击日志分析的spark项目
|java
    |conf：存放获取配置信息的工具类，使用Properties通过给定的参数读取配置文件，获取相应的参数
    |constants:存放常量信息的类，定义了项目中使用到的所有常量
    |dao：dao层，存放了进行mysql操作的所有接口与实现类
    |domain：相当于javabean，模板类
    |jdbc:用来jdbc的工具类
    |model：存放了实时分析中用到的模板类
    |test：用来生成模拟数据
    |util: 存放了项目中常用到的工具了类，有StringUtils，DateUtils。。。
 |scala
    |spark
    |accumulator:存放累加器的类
    |session：进行session分析的类
    |util：使用spark进行分析时用到的类
