{{- define "restic-rados-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "restic-rados-server.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "restic-rados-server.labels" -}}
helm.sh/chart: {{ include "restic-rados-server.chart" . }}
{{ include "restic-rados-server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "restic-rados-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "restic-rados-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "restic-rados-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "restic-rados-server.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- /* Release image tags carry a v prefix (vX.Y.Z). */ -}}
{{- define "restic-rados-server.image" -}}
{{- $tag := default (printf "v%s" .Chart.AppVersion) .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{- define "restic-rados-server.httpPort" -}}
{{- $listener := first .Values.config.listen -}}
{{- $address := $listener -}}
{{- if kindIs "map" $listener -}}
{{- $address = get $listener "endpoint" -}}
{{- end -}}
{{- last (splitList ":" (toString $address)) -}}
{{- end -}}

{{- define "restic-rados-server.serviceName" -}}
{{- $baseLength := sub 62 (len .name) | int -}}
{{- $base := include "restic-rados-server.fullname" .root | trunc $baseLength | trimSuffix "-" -}}
{{- printf "%s-%s" $base .name -}}
{{- end -}}

{{- define "restic-rados-server.networkPolicyName" -}}
{{- if .Values.services -}}
{{- include "restic-rados-server.serviceName" (dict "root" . "name" "default-deny") -}}
{{- else -}}
{{- include "restic-rados-server.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.servicePort" -}}
{{- if hasKey .service "port" -}}
{{- int .service.port -}}
{{- else -}}
{{- int .service.targetPort -}}
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.serviceAccess" -}}
{{- if hasKey .service "access" -}}
{{- $access := .service.access | toString -}}
{{- get (dict "read-only" "r" "read-append" "ra" "read-write" "rw") $access | default $access -}}
{{- else -}}
rw
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.probePort" -}}
{{- $name := first (keys .Values.services | sortAlpha) -}}
{{- int (index .Values.services $name).targetPort -}}
{{- end -}}

{{- define "restic-rados-server.metricsEnabled" -}}
{{- ternary "true" "" (dig "enabled" false (.Values.metrics | default dict)) -}}
{{- end -}}

{{- define "restic-rados-server.metricsPort" -}}
{{- int (dig "port" 9925 (.Values.metrics | default dict)) -}}
{{- end -}}

{{- define "restic-rados-server.metricsInjected" -}}
{{- if and .Values.services (eq (include "restic-rados-server.metricsEnabled" .) "true") -}}
{{- $declared := false -}}
{{- range $name, $service := .Values.services -}}
{{- if or (eq $name "metrics") (and (kindIs "map" $service) (hasKey $service "metrics")) -}}
{{- $declared = true -}}
{{- end -}}
{{- end -}}
{{- if not $declared -}}true{{- end -}}
{{- end -}}
{{- end -}}

{{- /* The services map plus the default metrics entry injected in services
mode; a services entry that configures metrics itself takes its place. */ -}}
{{- define "restic-rados-server.services" -}}
{{- $services := deepCopy (.Values.services | default dict) -}}
{{- if eq (include "restic-rados-server.metricsInjected" .) "true" -}}
{{- $metrics := .Values.metrics | default dict -}}
{{- $entry := dict "targetPort" (include "restic-rados-server.metricsPort" . | int) "access" "r" "metrics" "/metrics" "type" "ClusterIP" -}}
{{- with dig "networkPolicy" "ingressFrom" (list) $metrics -}}
{{- $_ := set $entry "networkPolicy" (dict "ingressFrom" .) -}}
{{- end -}}
{{- $_ := set $services "metrics" $entry -}}
{{- end -}}
{{- toJson $services -}}
{{- end -}}

{{- /* The path and port serving Prometheus metrics as JSON, or {} when none
is served; the alphabetically first metrics-serving services entry wins. */ -}}
{{- define "restic-rados-server.metricsScrapeTarget" -}}
{{- $target := dict -}}
{{- if .Values.services -}}
{{- range $name, $service := include "restic-rados-server.services" . | fromJson -}}
{{- if and (not $target) (kindIs "map" $service) (hasKey $service "metrics") -}}
{{- $target = dict "path" $service.metrics "port" (int $service.targetPort) -}}
{{- end -}}
{{- end -}}
{{- else if eq (include "restic-rados-server.metricsEnabled" .) "true" -}}
{{- $target = dict "path" "/metrics" "port" (include "restic-rados-server.metricsPort" . | int) -}}
{{- end -}}
{{- toJson $target -}}
{{- end -}}

{{- define "restic-rados-server.metricsScrapePath" -}}
{{- dig "path" "" (include "restic-rados-server.metricsScrapeTarget" . | fromJson) -}}
{{- end -}}

{{- define "restic-rados-server.metricsServed" -}}
{{- if include "restic-rados-server.metricsScrapePath" . -}}true{{- end -}}
{{- end -}}

{{- define "restic-rados-server.metricsScrapePort" -}}
{{- with dig "port" "" (include "restic-rados-server.metricsScrapeTarget" . | fromJson) -}}
{{- int . -}}
{{- end -}}
{{- end -}}

{{- /* Render ceph.conf from the ceph block. */ -}}
{{- define "restic-rados-server.cephConf" -}}
[global]
{{- if .Values.ceph.clusterID }}
fsid = {{ .Values.ceph.clusterID }}
{{- end }}
{{- if .Values.ceph.monitors }}
mon_host = {{ join "," .Values.ceph.monitors }}
{{- end }}
{{- end -}}

{{- /* Render the server config, injecting the ceph connectivity settings. */ -}}
{{- define "restic-rados-server.configJson" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- if .Values.services -}}
{{- $services := include "restic-rados-server.services" . | fromJson -}}
{{- $listeners := list -}}
{{- range $name, $service := $services -}}
{{- $listener := dict "endpoint" (printf "0.0.0.0:%d" (int $service.targetPort)) "policy" (dict "access" (include "restic-rados-server.serviceAccess" (dict "service" $service))) -}}
{{- if hasKey $service "metrics" -}}
{{- $_ := set $listener "metrics" $service.metrics -}}
{{- end -}}
{{- $listeners = append $listeners $listener -}}
{{- end -}}
{{- $_ := set $cfg "listen" $listeners -}}
{{- else if eq (include "restic-rados-server.metricsEnabled" .) "true" -}}
{{- $listeners := list -}}
{{- range $entry := .Values.config.listen -}}
{{- $listeners = append $listeners $entry -}}
{{- end -}}
{{- $listeners = append $listeners (dict "endpoint" (printf "0.0.0.0:%d" (include "restic-rados-server.metricsPort" . | int)) "policy" (dict "access" "r") "metrics" "/metrics") -}}
{{- $_ := set $cfg "listen" $listeners -}}
{{- end -}}
{{- $_ := set $cfg "ceph_conf" "/etc/ceph/ceph.conf" -}}
{{- if .Values.ceph.keyring.secret.name -}}
{{- $_ := set $cfg "keyring" "/etc/ceph/ceph.keyring" -}}
{{- end -}}
{{- if .Values.ceph.clientID -}}
{{- $_ := set $cfg "client_id" .Values.ceph.clientID -}}
{{- end -}}
{{ toPrettyJson $cfg }}
{{- end -}}

{{- define "restic-rados-server.networkPolicyEgressDefaults" -}}
{{- $dns := dict "enabled" true "to" (list) -}}
{{- $monitors := dict "enabled" true "ports" (list 3300 6789) "to" (list) -}}
{{- $portRange := dict "from" 6800 "to" 7568 -}}
{{- $osds := dict "enabled" true "portRange" $portRange "to" (list) -}}
{{- $ceph := dict "monitors" $monitors "osds" $osds -}}
{{- dict "ceph" $ceph "dns" $dns "enabled" false "rules" (list) | toYaml -}}
{{- end -}}

{{- define "restic-rados-server.validateNetworkPolicyLabelSelector" -}}
{{- $path := .path -}}
{{- $selector := .selector -}}
{{- if not (kindIs "map" $selector) -}}
{{- fail (printf "%s must be an object" $path) -}}
{{- end -}}
{{- range $field := keys $selector -}}
{{- if not (has $field (list "matchExpressions" "matchLabels")) -}}
{{- fail (printf "%s has unknown field %q" $path $field) -}}
{{- end -}}
{{- end -}}
{{- if hasKey $selector "matchLabels" -}}
{{- $labels := get $selector "matchLabels" -}}
{{- if not (kindIs "map" $labels) -}}
{{- fail (printf "%s.matchLabels must be an object" $path) -}}
{{- end -}}
{{- range $label, $value := $labels -}}
{{- if not (kindIs "string" $value) -}}
{{- fail (printf "%s.matchLabels[%q] must be a string" $path $label) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if hasKey $selector "matchExpressions" -}}
{{- $expressions := get $selector "matchExpressions" -}}
{{- if not (kindIs "slice" $expressions) -}}
{{- fail (printf "%s.matchExpressions must be a list" $path) -}}
{{- end -}}
{{- range $index, $expression := $expressions -}}
{{- if not (kindIs "map" $expression) -}}
{{- fail (printf "%s.matchExpressions[%d] must be an object" $path $index) -}}
{{- end -}}
{{- range $field := keys $expression -}}
{{- if not (has $field (list "key" "operator" "values")) -}}
{{- fail (printf "%s.matchExpressions[%d] has unknown field %q" $path $index $field) -}}
{{- end -}}
{{- end -}}
{{- if or (not (hasKey $expression "key")) (not (hasKey $expression "operator")) -}}
{{- fail (printf "%s.matchExpressions[%d] requires key and operator" $path $index) -}}
{{- end -}}
{{- if or (not (kindIs "string" $expression.key)) (not (kindIs "string" $expression.operator)) -}}
{{- fail (printf "%s.matchExpressions[%d].key and operator must be strings" $path $index) -}}
{{- end -}}
{{- if hasKey $expression "values" -}}
{{- if not (kindIs "slice" $expression.values) -}}
{{- fail (printf "%s.matchExpressions[%d].values must be a list" $path $index) -}}
{{- end -}}
{{- range $valueIndex, $value := $expression.values -}}
{{- if not (kindIs "string" $value) -}}
{{- fail (printf "%s.matchExpressions[%d].values[%d] must be a string" $path $index $valueIndex) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.validateNetworkPolicyPeers" -}}
{{- $path := .path -}}
{{- range $index, $peer := .peers -}}
{{- if not (kindIs "map" $peer) -}}
{{- fail (printf "%s[%d] must be an object" $path $index) -}}
{{- end -}}
{{- range $field := keys $peer -}}
{{- if not (has $field (list "ipBlock" "namespaceSelector" "podSelector")) -}}
{{- fail (printf "%s[%d] has unknown field %q" $path $index $field) -}}
{{- end -}}
{{- end -}}
{{- $hasIPBlock := hasKey $peer "ipBlock" -}}
{{- $hasNamespaceSelector := hasKey $peer "namespaceSelector" -}}
{{- $hasPodSelector := hasKey $peer "podSelector" -}}
{{- if $hasIPBlock -}}
{{- $ipBlock := get $peer "ipBlock" -}}
{{- if not (kindIs "map" $ipBlock) -}}
{{- fail (printf "%s[%d].ipBlock must be an object" $path $index) -}}
{{- end -}}
{{- range $field := keys $ipBlock -}}
{{- if not (has $field (list "cidr" "except")) -}}
{{- fail (printf "%s[%d].ipBlock has unknown field %q" $path $index $field) -}}
{{- end -}}
{{- end -}}
{{- if or (not (hasKey $ipBlock "cidr")) (not (kindIs "string" $ipBlock.cidr)) -}}
{{- fail (printf "%s[%d].ipBlock.cidr must be a string" $path $index) -}}
{{- end -}}
{{- if hasKey $ipBlock "except" -}}
{{- if not (kindIs "slice" $ipBlock.except) -}}
{{- fail (printf "%s[%d].ipBlock.except must be a list" $path $index) -}}
{{- end -}}
{{- range $exceptIndex, $except := $ipBlock.except -}}
{{- if not (kindIs "string" $except) -}}
{{- fail (printf "%s[%d].ipBlock.except[%d] must be a string" $path $index $exceptIndex) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if $hasNamespaceSelector -}}
{{- include "restic-rados-server.validateNetworkPolicyLabelSelector" (dict "path" (printf "%s[%d].namespaceSelector" $path $index) "selector" (get $peer "namespaceSelector")) -}}
{{- end -}}
{{- if $hasPodSelector -}}
{{- include "restic-rados-server.validateNetworkPolicyLabelSelector" (dict "path" (printf "%s[%d].podSelector" $path $index) "selector" (get $peer "podSelector")) -}}
{{- end -}}
{{- if not (or $hasIPBlock $hasNamespaceSelector $hasPodSelector) -}}
{{- fail (printf "%s[%d] must specify ipBlock, namespaceSelector, or podSelector" $path $index) -}}
{{- end -}}
{{- if and $hasIPBlock (or $hasNamespaceSelector $hasPodSelector) -}}
{{- fail (printf "%s[%d].ipBlock cannot be combined with namespaceSelector or podSelector" $path $index) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.validateNetworkPolicyRules" -}}
{{- $path := .path -}}
{{- $fields := .fields -}}
{{- $peerField := .peerField -}}
{{- range $index, $rule := .rules -}}
{{- if not (kindIs "map" $rule) -}}
{{- fail (printf "%s[%d] must be an object" $path $index) -}}
{{- end -}}
{{- range $field := keys $rule -}}
{{- if not (has $field $fields) -}}
{{- fail (printf "%s[%d] has unknown field %q" $path $index $field) -}}
{{- end -}}
{{- end -}}
{{- if hasKey $rule $peerField -}}
{{- $peers := get $rule $peerField -}}
{{- if not (kindIs "slice" $peers) -}}
{{- fail (printf "%s[%d].%s must be a list" $path $index $peerField) -}}
{{- end -}}
{{- include "restic-rados-server.validateNetworkPolicyPeers" (dict "path" (printf "%s[%d].%s" $path $index $peerField) "peers" $peers) -}}
{{- end -}}
{{- if hasKey $rule "ports" -}}
{{- $ports := get $rule "ports" -}}
{{- if not (kindIs "slice" $ports) -}}
{{- fail (printf "%s[%d].ports must be a list" $path $index) -}}
{{- end -}}
{{- range $portIndex, $port := $ports -}}
{{- if not (kindIs "map" $port) -}}
{{- fail (printf "%s[%d].ports[%d] must be an object" $path $index $portIndex) -}}
{{- end -}}
{{- range $field := keys $port -}}
{{- if not (has $field (list "endPort" "port" "protocol")) -}}
{{- fail (printf "%s[%d].ports[%d] has unknown field %q" $path $index $portIndex $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.validate" -}}
{{- if and (not .Values.services) (not .Values.config.listen) -}}
{{- fail "config.listen must not be empty" -}}
{{- end -}}
{{- $metricsConfig := .Values.metrics | default dict -}}
{{- if not (kindIs "map" $metricsConfig) -}}
{{- fail "metrics must be an object" -}}
{{- end -}}
{{- range $field := keys $metricsConfig -}}
{{- if not (has $field (list "enabled" "networkPolicy" "port")) -}}
{{- fail (printf "metrics has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if and (hasKey $metricsConfig "enabled") (not (kindIs "bool" $metricsConfig.enabled)) -}}
{{- fail "metrics.enabled must be a boolean" -}}
{{- end -}}
{{- if hasKey $metricsConfig "port" -}}
{{- if not (regexMatch "^[0-9]+$" (toString $metricsConfig.port)) -}}
{{- fail "metrics.port must be an integer" -}}
{{- end -}}
{{- $metricsPort := int $metricsConfig.port -}}
{{- if or (lt $metricsPort 1) (gt $metricsPort 65535) -}}
{{- fail "metrics.port must be between 1 and 65535" -}}
{{- end -}}
{{- end -}}
{{- if hasKey $metricsConfig "networkPolicy" -}}
{{- if not (kindIs "map" $metricsConfig.networkPolicy) -}}
{{- fail "metrics.networkPolicy must be an object" -}}
{{- end -}}
{{- range $field := keys $metricsConfig.networkPolicy -}}
{{- if ne $field "ingressFrom" -}}
{{- fail (printf "metrics.networkPolicy has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- $metricsIngressFrom := dig "networkPolicy" "ingressFrom" (list) $metricsConfig -}}
{{- if not (kindIs "slice" $metricsIngressFrom) -}}
{{- fail "metrics.networkPolicy.ingressFrom must be a list" -}}
{{- end -}}
{{- if .Values.networkPolicy.enabled -}}
{{- include "restic-rados-server.validateNetworkPolicyPeers" (dict "path" "metrics.networkPolicy.ingressFrom" "peers" $metricsIngressFrom) -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "bool" .Values.prometheusScrape) -}}
{{- fail "prometheusScrape must be a boolean" -}}
{{- end -}}
{{- $serviceMonitor := .Values.serviceMonitor | default dict -}}
{{- if not (kindIs "map" $serviceMonitor) -}}
{{- fail "serviceMonitor must be an object" -}}
{{- end -}}
{{- range $field := keys $serviceMonitor -}}
{{- if not (has $field (list "enabled" "interval" "jobLabel" "labels" "metricRelabelings" "relabelings" "scrapeTimeout")) -}}
{{- fail (printf "serviceMonitor has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if and (hasKey $serviceMonitor "enabled") (not (kindIs "bool" $serviceMonitor.enabled)) -}}
{{- fail "serviceMonitor.enabled must be a boolean" -}}
{{- end -}}
{{- if and (dig "enabled" false $serviceMonitor) (ne (include "restic-rados-server.metricsServed" .) "true") -}}
{{- fail "serviceMonitor.enabled requires served metrics; enable metrics or add a metrics path to a services entry" -}}
{{- end -}}
{{- if and (dig "enabled" false $serviceMonitor) .Values.services -}}
{{- $metricsPaths := dict -}}
{{- range $name, $service := include "restic-rados-server.services" . | fromJson -}}
{{- if and (kindIs "map" $service) (hasKey $service "metrics") -}}
{{- $_ := set $metricsPaths (toString $service.metrics) $name -}}
{{- end -}}
{{- end -}}
{{- if gt (len (keys $metricsPaths)) 1 -}}
{{- fail "serviceMonitor requires a single metrics path across services; align services.*.metrics or disable serviceMonitor" -}}
{{- end -}}
{{- end -}}
{{- if and (not .Values.services) (eq (include "restic-rados-server.metricsEnabled" .) "true") -}}
{{- $metricsPortValue := include "restic-rados-server.metricsPort" . -}}
{{- range $entry := .Values.config.listen -}}
{{- $address := $entry -}}
{{- if kindIs "map" $entry -}}
{{- $address = get $entry "endpoint" -}}
{{- end -}}
{{- if eq (last (splitList ":" (toString $address))) $metricsPortValue -}}
{{- fail (printf "metrics.port %s is already used by a config.listen endpoint; change metrics.port" $metricsPortValue) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if and (eq (include "restic-rados-server.metricsEnabled" .) "true") .Values.services (hasKey .Values.services "metrics") -}}
{{- $metricsEntry := get .Values.services "metrics" -}}
{{- if and (kindIs "map" $metricsEntry) (not (hasKey $metricsEntry "metrics")) -}}
{{- fail "services.metrics replaces the chart-managed metrics entry and must set a metrics path when metrics.enabled is true" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.services (dig "networkPolicy" "ingressFrom" (list) $metricsConfig) (ne (include "restic-rados-server.metricsInjected" .) "true") -}}
{{- $declaredMetrics := false -}}
{{- range $name, $service := .Values.services -}}
{{- if or (eq $name "metrics") (and (kindIs "map" $service) (hasKey $service "metrics")) -}}
{{- $declaredMetrics = true -}}
{{- end -}}
{{- end -}}
{{- if $declaredMetrics -}}
{{- fail "metrics.networkPolicy.ingressFrom is ignored when a services entry provides metrics; set services.<name>.networkPolicy.ingressFrom instead" -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "map" .Values.networkPolicy) -}}
{{- fail "networkPolicy must be an object" -}}
{{- end -}}
{{- range $field := keys .Values.networkPolicy -}}
{{- if not (has $field (list "enabled" "egress" "ingress" "ingressFrom")) -}}
{{- fail (printf "networkPolicy has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "bool" .Values.networkPolicy.enabled) -}}
{{- fail "networkPolicy.enabled must be a boolean" -}}
{{- end -}}
{{- if .Values.networkPolicy.enabled -}}
{{- $ingressFrom := dig "ingressFrom" (list) .Values.networkPolicy -}}
{{- if not (kindIs "slice" $ingressFrom) -}}
{{- fail "networkPolicy.ingressFrom must be a list" -}}
{{- end -}}
{{- include "restic-rados-server.validateNetworkPolicyPeers" (dict "path" "networkPolicy.ingressFrom" "peers" $ingressFrom) -}}
{{- $ingress := dig "ingress" (list) .Values.networkPolicy -}}
{{- if not (kindIs "slice" $ingress) -}}
{{- fail "networkPolicy.ingress must be a list" -}}
{{- end -}}
{{- include "restic-rados-server.validateNetworkPolicyRules" (dict "fields" (list "from" "ports") "path" "networkPolicy.ingress" "peerField" "from" "rules" $ingress) -}}
{{- $configuredEgress := dig "egress" (dict) .Values.networkPolicy -}}
{{- if not (kindIs "map" $configuredEgress) -}}
{{- fail "networkPolicy.egress must be an object" -}}
{{- end -}}
{{- range $field := keys $configuredEgress -}}
{{- if not (has $field (list "ceph" "dns" "enabled" "rules")) -}}
{{- fail (printf "networkPolicy.egress has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- $egressDefaults := include "restic-rados-server.networkPolicyEgressDefaults" . | fromYaml -}}
{{- $egress := mergeOverwrite $egressDefaults $configuredEgress -}}
{{- if not (kindIs "bool" $egress.enabled) -}}
{{- fail "networkPolicy.egress.enabled must be a boolean" -}}
{{- end -}}
{{- if $egress.enabled -}}
{{- if or (not (hasKey $configuredEgress "enabled")) (not (hasKey $configuredEgress "rules")) (not (hasKey $configuredEgress "dns")) (not (hasKey $configuredEgress "ceph")) -}}
{{- fail "networkPolicy.egress requires enabled, rules, dns, and ceph when enabled" -}}
{{- end -}}
{{- $egress = $configuredEgress -}}
{{- if not (kindIs "slice" $egress.rules) -}}
{{- fail "networkPolicy.egress.rules must be a list" -}}
{{- end -}}
{{- include "restic-rados-server.validateNetworkPolicyRules" (dict "fields" (list "ports" "to") "path" "networkPolicy.egress.rules" "peerField" "to" "rules" $egress.rules) -}}
{{- if not (kindIs "map" $egress.dns) -}}
{{- fail "networkPolicy.egress.dns must be an object" -}}
{{- end -}}
{{- if or (not (hasKey $egress.dns "enabled")) (not (hasKey $egress.dns "to")) -}}
{{- fail "networkPolicy.egress.dns requires enabled and to" -}}
{{- end -}}
{{- range $field := keys $egress.dns -}}
{{- if not (has $field (list "enabled" "to")) -}}
{{- fail (printf "networkPolicy.egress.dns has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "bool" $egress.dns.enabled) -}}
{{- fail "networkPolicy.egress.dns.enabled must be a boolean" -}}
{{- end -}}
{{- if $egress.dns.enabled -}}
{{- if not (kindIs "slice" $egress.dns.to) -}}
{{- fail "networkPolicy.egress.dns.to must be a list" -}}
{{- end -}}
{{- include "restic-rados-server.validateNetworkPolicyPeers" (dict "path" "networkPolicy.egress.dns.to" "peers" $egress.dns.to) -}}
{{- end -}}
{{- if not (kindIs "map" $egress.ceph) -}}
{{- fail "networkPolicy.egress.ceph must be an object" -}}
{{- end -}}
{{- if or (not (hasKey $egress.ceph "monitors")) (not (hasKey $egress.ceph "osds")) -}}
{{- fail "networkPolicy.egress.ceph requires monitors and osds" -}}
{{- end -}}
{{- range $field := keys $egress.ceph -}}
{{- if not (has $field (list "monitors" "osds")) -}}
{{- fail (printf "networkPolicy.egress.ceph has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "map" $egress.ceph.monitors) -}}
{{- fail "networkPolicy.egress.ceph.monitors must be an object" -}}
{{- end -}}
{{- if or (not (hasKey $egress.ceph.monitors "enabled")) (not (hasKey $egress.ceph.monitors "ports")) (not (hasKey $egress.ceph.monitors "to")) -}}
{{- fail "networkPolicy.egress.ceph.monitors requires enabled, ports, and to" -}}
{{- end -}}
{{- $monitors := $egress.ceph.monitors -}}
{{- range $field := keys $monitors -}}
{{- if not (has $field (list "enabled" "ports" "to")) -}}
{{- fail (printf "networkPolicy.egress.ceph.monitors has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "bool" $monitors.enabled) -}}
{{- fail "networkPolicy.egress.ceph.monitors.enabled must be a boolean" -}}
{{- end -}}
{{- if $monitors.enabled -}}
{{- if not (kindIs "slice" $monitors.to) -}}
{{- fail "networkPolicy.egress.ceph.monitors.to must be a list" -}}
{{- end -}}
{{- include "restic-rados-server.validateNetworkPolicyPeers" (dict "path" "networkPolicy.egress.ceph.monitors.to" "peers" $monitors.to) -}}
{{- if not (kindIs "slice" $monitors.ports) -}}
{{- fail "networkPolicy.egress.ceph.monitors.ports must be a non-empty list" -}}
{{- end -}}
{{- if eq (len $monitors.ports) 0 -}}
{{- fail "networkPolicy.egress.ceph.monitors.ports must be a non-empty list" -}}
{{- end -}}
{{- range $port := $monitors.ports -}}
{{- if not (regexMatch "^[0-9]+$" (toString $port)) -}}
{{- fail "networkPolicy.egress.ceph.monitors.ports entries must be integers" -}}
{{- end -}}
{{- $portNumber := atoi (toString $port) -}}
{{- if or (lt $portNumber 1) (gt $portNumber 65535) -}}
{{- fail "networkPolicy.egress.ceph.monitors.ports entries must be between 1 and 65535" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "map" $egress.ceph.osds) -}}
{{- fail "networkPolicy.egress.ceph.osds must be an object" -}}
{{- end -}}
{{- if or (not (hasKey $egress.ceph.osds "enabled")) (not (hasKey $egress.ceph.osds "portRange")) (not (hasKey $egress.ceph.osds "to")) -}}
{{- fail "networkPolicy.egress.ceph.osds requires enabled, portRange, and to" -}}
{{- end -}}
{{- $osds := $egress.ceph.osds -}}
{{- range $field := keys $osds -}}
{{- if not (has $field (list "enabled" "portRange" "to")) -}}
{{- fail (printf "networkPolicy.egress.ceph.osds has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if not (kindIs "bool" $osds.enabled) -}}
{{- fail "networkPolicy.egress.ceph.osds.enabled must be a boolean" -}}
{{- end -}}
{{- if $osds.enabled -}}
{{- if not (kindIs "slice" $osds.to) -}}
{{- fail "networkPolicy.egress.ceph.osds.to must be a list" -}}
{{- end -}}
{{- include "restic-rados-server.validateNetworkPolicyPeers" (dict "path" "networkPolicy.egress.ceph.osds.to" "peers" $osds.to) -}}
{{- if not (kindIs "map" $osds.portRange) -}}
{{- fail "networkPolicy.egress.ceph.osds.portRange must be an object" -}}
{{- end -}}
{{- range $field := keys $osds.portRange -}}
{{- if not (has $field (list "from" "to")) -}}
{{- fail (printf "networkPolicy.egress.ceph.osds.portRange has unknown field %q" $field) -}}
{{- end -}}
{{- end -}}
{{- if or (not (hasKey $osds.portRange "from")) (not (hasKey $osds.portRange "to")) -}}
{{- fail "networkPolicy.egress.ceph.osds.portRange requires from and to" -}}
{{- end -}}
{{- if or (not (regexMatch "^[0-9]+$" (toString $osds.portRange.from))) (not (regexMatch "^[0-9]+$" (toString $osds.portRange.to))) -}}
{{- fail "networkPolicy.egress.ceph.osds.portRange.from and to must be integers" -}}
{{- end -}}
{{- $from := atoi (toString $osds.portRange.from) -}}
{{- $to := atoi (toString $osds.portRange.to) -}}
{{- if or (lt $from 1) (gt $from 65535) (lt $to 1) (gt $to 65535) -}}
{{- fail "networkPolicy.egress.ceph.osds.portRange.from and to must be between 1 and 65535" -}}
{{- end -}}
{{- if gt $from $to -}}
{{- fail "networkPolicy.egress.ceph.osds.portRange.to must be greater than or equal to from" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if .Values.services -}}
{{- if and .Values.networkPolicy.enabled .Values.networkPolicy.ingressFrom -}}
{{- fail "networkPolicy.ingressFrom must be empty when services is configured; use services.<name>.networkPolicy.ingressFrom" -}}
{{- end -}}
{{- if and .Values.networkPolicy.enabled .Values.networkPolicy.ingress -}}
{{- fail "networkPolicy.ingress must be empty when services is configured; use services.<name>.networkPolicy.ingressFrom" -}}
{{- end -}}
{{- $targetPorts := dict -}}
{{- $serviceNames := dict -}}
{{- $networkPolicyNames := dict -}}
{{- if .Values.networkPolicy.enabled -}}
{{- $_ := set $networkPolicyNames (include "restic-rados-server.networkPolicyName" .) "the base NetworkPolicy" -}}
{{- end -}}
{{- range $name, $service := .Values.services -}}
{{- if or (gt (len $name) 61) (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name)) -}}
{{- fail (printf "services key %q must be a DNS label no longer than 61 characters" $name) -}}
{{- end -}}
{{- if eq $name "default-deny" -}}
{{- fail "services key \"default-deny\" is reserved for the base NetworkPolicy" -}}
{{- end -}}
{{- $resourceName := include "restic-rados-server.serviceName" (dict "root" $ "name" $name) -}}
{{- if hasKey $serviceNames $resourceName -}}
{{- fail (printf "services.%s and services.%s render the same Service name %q" (get $serviceNames $resourceName) $name $resourceName) -}}
{{- end -}}
{{- $_ := set $serviceNames $resourceName $name -}}
{{- if not (kindIs "map" $service) -}}
{{- fail (printf "services.%s must be an object" $name) -}}
{{- end -}}
{{- range $field := keys $service -}}
{{- if not (has $field (list "access" "annotations" "metrics" "networkPolicy" "port" "targetPort" "type")) -}}
{{- fail (printf "services.%s has unknown field %q" $name $field) -}}
{{- end -}}
{{- end -}}
{{- if hasKey $service "metrics" -}}
{{- if or (not (kindIs "string" $service.metrics)) (not (hasPrefix "/" $service.metrics)) (eq $service.metrics "/") (hasSuffix "/" $service.metrics) (has $service.metrics (list "/healthz" "/readyz")) -}}
{{- fail (printf "services.%s.metrics must be a path starting with \"/\"" $name) -}}
{{- end -}}
{{- end -}}
{{- if and (hasKey $service "annotations") (not (kindIs "map" $service.annotations)) -}}
{{- fail (printf "services.%s.annotations must be an object" $name) -}}
{{- end -}}
{{- if and (hasKey $service "type") (not (kindIs "string" $service.type)) -}}
{{- fail (printf "services.%s.type must be a string" $name) -}}
{{- end -}}
{{- if hasKey $service "networkPolicy" -}}
{{- if not (kindIs "map" $service.networkPolicy) -}}
{{- fail (printf "services.%s.networkPolicy must be an object" $name) -}}
{{- end -}}
{{- range $field := keys $service.networkPolicy -}}
{{- if ne $field "ingressFrom" -}}
{{- fail (printf "services.%s.networkPolicy has unknown field %q" $name $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if not (hasKey $service "targetPort") -}}
{{- fail (printf "services.%s.targetPort is required" $name) -}}
{{- end -}}
{{- if not (regexMatch "^[0-9]+$" (toString $service.targetPort)) -}}
{{- fail (printf "services.%s.targetPort must be an integer" $name) -}}
{{- end -}}
{{- $targetPort := int $service.targetPort -}}
{{- if or (lt $targetPort 1) (gt $targetPort 65535) -}}
{{- fail (printf "services.%s.targetPort must be between 1 and 65535" $name) -}}
{{- end -}}
{{- $targetPortKey := printf "%d" $targetPort -}}
{{- if hasKey $targetPorts $targetPortKey -}}
{{- fail (printf "services.%s.targetPort %d is already used by services.%s" $name $targetPort (get $targetPorts $targetPortKey)) -}}
{{- end -}}
{{- $_ := set $targetPorts $targetPortKey $name -}}
{{- if and (hasKey $service "port") (not (regexMatch "^[0-9]+$" (toString $service.port))) -}}
{{- fail (printf "services.%s.port must be an integer" $name) -}}
{{- end -}}
{{- $port := include "restic-rados-server.servicePort" (dict "service" $service) | int -}}
{{- if or (lt $port 1) (gt $port 65535) -}}
{{- fail (printf "services.%s.port must be between 1 and 65535" $name) -}}
{{- end -}}
{{- $access := include "restic-rados-server.serviceAccess" (dict "service" $service) -}}
{{- if not (has $access (list "r" "ra" "rw")) -}}
{{- fail (printf "services.%s.access must be one of r, read-only, ra, read-append, rw, or read-write" $name) -}}
{{- end -}}
{{- $ingressFrom := dig "networkPolicy" "ingressFrom" (list) $service -}}
{{- if not (kindIs "slice" $ingressFrom) -}}
{{- fail (printf "services.%s.networkPolicy.ingressFrom must be a list" $name) -}}
{{- end -}}
{{- if $.Values.networkPolicy.enabled -}}
{{- include "restic-rados-server.validateNetworkPolicyPeers" (dict "path" (printf "services.%s.networkPolicy.ingressFrom" $name) "peers" $ingressFrom) -}}
{{- end -}}
{{- if and $.Values.networkPolicy.enabled (gt (len $ingressFrom) 0) -}}
{{- if hasKey $networkPolicyNames $resourceName -}}
{{- fail (printf "services.%s NetworkPolicy name %q conflicts with %s" $name $resourceName (get $networkPolicyNames $resourceName)) -}}
{{- end -}}
{{- $_ := set $networkPolicyNames $resourceName (printf "services.%s" $name) -}}
{{- end -}}
{{- end -}}
{{- if eq (include "restic-rados-server.metricsInjected" .) "true" -}}
{{- $metricsPortKey := include "restic-rados-server.metricsPort" . -}}
{{- if hasKey $targetPorts $metricsPortKey -}}
{{- fail (printf "metrics.port %s is already used by services.%s; change metrics.port or set metrics.enabled: false" $metricsPortKey (get $targetPorts $metricsPortKey)) -}}
{{- end -}}
{{- end -}}
{{- else -}}
{{- if not (regexMatch "^[0-9]+$" (include "restic-rados-server.httpPort" .)) -}}
{{- fail (printf "config.listen[0] must be a TCP host:port endpoint, got %v" (first .Values.config.listen)) -}}
{{- end -}}
{{- end -}}
{{- range $name, $repo := .Values.config.repos -}}
{{- if not (or $repo.pools $repo.blob_pools) -}}
{{- fail (printf "repo %q has no pools configured (set config.repos.%s.pools, or remove the repo by setting it to null)" $name $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
