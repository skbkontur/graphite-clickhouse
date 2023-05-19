# expansion
package for GO graphite globs expansion

## Usage
It contains the only public functions `Expand(in string, max int) ([]string, error)`, that expands shell-like expressions `1{c,e}2[b-d]` to `['1c2b', '1c2c', '1c2d', '1e2b', '1e2c', '1e2d']` (if max = -1).
max for restict max expanded results, > 0 - restuct  max expamnded results, 0 - disables expand, -1 - unlimited, -2 - expand only first node, -3 - expand only two nodes, etc.
