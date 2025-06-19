
    @DeleteMapping("/cancelScheduledWorkflow")
    public ResponseEntity<ApiResponse<String>> cancelScheduledWorkflow(@RequestParam String instanceId) {
        List<ScheduleWorkflow> scheduled = scheduleWorkflowService.findByRunningInstanceId(instanceId);
        if (scheduled == null || scheduled.isEmpty()) {
            return new ResponseEntity<>(
                    ResponseUtil.error(HttpStatus.NOT_FOUND.value(), "No scheduled workflow found for instanceId",
                            null),
                    HttpStatus.NOT_FOUND);
        }
        scheduled.forEach(sw -> scheduleWorkflowService.deleteById(sw.getId()));
        return new ResponseEntity<>(
                ResponseUtil.success(HttpStatus.OK.value(), "Scheduled workflow(s) cancelled and deleted", instanceId),
                HttpStatus.OK);
    }
