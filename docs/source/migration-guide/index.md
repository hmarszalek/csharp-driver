# Migration Guide

This guide helps you migrate from the DataStax C# Driver (or ScyllaDB's fork) to the new C# RS Driver, which is an API-compatible rewrite of the C# driver as a wrapper over the Rust driver.

## Overview

The C# RS Driver aims to maintain API compatibility with the existing drivers while providing improved performance and features through the underlying Rust driver. Most of your existing code should work with minimal changes.

## Host API

### Deleted as no longer supported APIs

TODO

### Semantic/Syntactic incompatibilities

TODO

## Metadata API

### Semantic/Syntactic incompatibilities

**Important Behavioral Difference:**
In the DataStax C# driver, `Metadata` is accessible at the `Cluster` level without establishing a session. In C# RS Driver, you must create a `Session` before accessing `Metadata`, as the underlying Rust driver acquires `Metadata` information at the `Session` level.

**Migration Impact:** Ensure you establish a session before querying any metadata properties or methods.
