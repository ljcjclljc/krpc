#pragma once

// 文件用途：
// 对外暴露统一的版本号查询接口，供应用启动日志、测试输出和文档对齐使用。

#include <string>

namespace rpc::common {

// 返回当前构建的语义化版本号。
// 约定：仅用于展示与追踪，不参与业务逻辑分支判断。
std::string version();

}  // namespace rpc::common
