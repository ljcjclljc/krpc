#include "rpc/common/version.h"

// 文件用途：
// 提供统一版本号实现，避免各模块硬编码版本字符串。

namespace rpc::common {

std::string version() {
    // 当前项目版本。发布时由版本流程统一维护。
    return "0.1.0";
}

}  // namespace rpc::common
