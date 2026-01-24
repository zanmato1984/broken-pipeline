#pragma once

/**
 * @file logical_pipeline.h
 *
 * @brief Compatibility header.
 *
 * The public pipeline type is now `openpipeline::Pipeline`.
 * This header keeps the old `LogicalPipeline` name as an alias.
 */

#include <openpipeline/pipeline/pipeline.h>

namespace openpipeline {

template <OpenPipelineTraits Traits>
using LogicalPipeline = Pipeline<Traits>;

}  // namespace openpipeline
