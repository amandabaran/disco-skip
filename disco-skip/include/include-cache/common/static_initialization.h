#include "../common/thread_context.h"

thread_local thread_context *thread_context::this_ctx = nullptr;
