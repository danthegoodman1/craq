variable "gcp_project" {
  type = string
}

variable "gcp_region" {
  type = string
}

variable "run_id" {
  type = string
}

variable "topology" {
  type = string

  validation {
    condition     = contains(["single-zone", "multi-zone"], var.topology)
    error_message = "topology must be single-zone or multi-zone."
  }
}

variable "client_placement" {
  type = string

  validation {
    condition     = contains(["same-zone", "remote-zone"], var.client_placement)
    error_message = "client_placement must be same-zone or remote-zone."
  }
}

variable "operator_cidrs" {
  type = list(string)
}

variable "ssh_public_key" {
  type = string
}

variable "ssh_user" {
  type = string
}

variable "coordinator_machine_type" {
  type = string
}

variable "client_machine_type" {
  type = string
}

variable "storage_machine_type" {
  type = string

  validation {
    condition = contains([
      "c4a-standard-4-lssd",
      "c4a-standard-8-lssd",
      "c4a-standard-16-lssd",
      "c4a-standard-32-lssd",
      "c4a-standard-48-lssd",
      "c4a-standard-64-lssd",
      "c4a-standard-72-lssd",
      "c4a-highmem-4-lssd",
      "c4a-highmem-8-lssd",
      "c4a-highmem-16-lssd",
      "c4a-highmem-32-lssd",
      "c4a-highmem-48-lssd",
      "c4a-highmem-64-lssd",
      "c4a-highmem-72-lssd",
    ], var.storage_machine_type)
    error_message = "storage_machine_type must be a supported C4A Local SSD machine type."
  }
}

variable "coordinator_boot_disk_gib" {
  type = number
}
