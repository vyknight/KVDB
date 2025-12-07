//
// Created by Zekun Liu on 2025-12-07.
//

#include "Page.h"

Page::Page() : data_(PAGE_SIZE, 0) {
    load_time_ = std::time(nullptr);
    last_access_ = load_time_;
}