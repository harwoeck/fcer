// Package fcer is a file partition algorithm, that splits a file into a set of
// partitions and calculates their offset and length. After finding this parts
// it is possible to process a file concurrently without worker-interference.
//
// fcer is used for files that contain millions of lines separated by a new line
// '\n'. It can speed up file processing drastically compared to a single reader.
//
// Partitions are split into close-to-equal sizes. A single input line is never
// split between multiple parts. Each partition therefore ends with a new line.
package fcer
