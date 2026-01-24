#pragma once

/**
 * @file logical_pipeline.h
 *
 * @brief Compatibility header.
 *
 * The public pipeline type is now `openpipeline::pipeline::Pipeline`.
 * This header keeps the old `LogicalPipeline` name as an alias.
 */

#include <openpipeline/pipeline/pipeline.h>

namespace openpipeline::pipeline {

template <OpenPipelineTraits Traits>
using LogicalPipeline = Pipeline<Traits>;

}  // namespace openpipeline::pipeline
