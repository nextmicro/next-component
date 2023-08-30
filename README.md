# next-component

> next 框架组件库

## 组件

| 名称       | 代码                                                                    | 示例  | 文档  |
|----------|-----------------------------------------------------------------------|-----|-----|
| GORM     | [Code](https://github.com/nextmicro/next-component/-/tree/main/gorm)  | 待添加 | 待添加 |
| Go-Redis | [Code](https://github.com/nextmicro/next-component/-/tree/main/redis) | 待添加 | 待添加 |
| MongoDB  | [Code](https://github.com/nextmicro/next-component/-/tree/main/mongo) | 待添加 | 待添加 |

## 开发文档

组件库必须实现 `loader.Loader` 接口。实际 `loader.Loader` 接口是 NextMicro `loader.Loader` 接口，传递参数参考
组件库的 `options.go` 实现。