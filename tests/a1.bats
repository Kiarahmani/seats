#!/usr/bin/env zunit
# 
@setup {
  # Load a script to set up the test environment
  load tests_scprits.sh
}

@test '(1) anomaly occured?' {
  run a1
  assert "$output" in ".X.Y" ".Y.X" 
}
@test '(2) anomaly occured?' {
  run a1
  assert "$output" in ".X.Y" ".Y.X" 
}
@test '(3) anomaly occured?' {
  run a1
  assert "$output" in ".X.Y" ".Y.X" 
}
@test '(4) anomaly occured?' {
  run a1
  assert "$output" in ".X.Y" ".Y.X" 
}
@test '(5) anomaly occured?' {
  run a1
  assert "$output" in ".X.Y" ".Y.X" 
}
