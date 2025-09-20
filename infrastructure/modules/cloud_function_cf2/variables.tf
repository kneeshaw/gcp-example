variable "memory_limit" {
  description = "The amount of memory to allocate to the function."
  type        = string
}

variable "cpu_limit" {
  description = "The amount of CPU to allocate to the function (e.g., '1', '500m')."
  type        = string
}
