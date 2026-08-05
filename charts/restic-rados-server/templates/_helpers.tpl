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
{{- $address = get $listener "address" -}}
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
{{- .service.access | toString -}}
{{- else -}}
rw
{{- end -}}
{{- end -}}

{{- define "restic-rados-server.probePort" -}}
{{- $name := first (keys .Values.services | sortAlpha) -}}
{{- int (index .Values.services $name).targetPort -}}
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
{{- $listeners := list -}}
{{- range $name, $service := .Values.services -}}
{{- $listeners = append $listeners (dict "address" (printf "0.0.0.0:%d" (int $service.targetPort)) "access" (include "restic-rados-server.serviceAccess" (dict "service" $service))) -}}
{{- end -}}
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

{{- define "restic-rados-server.validate" -}}
{{- if and (not .Values.services) (not .Values.config.listen) -}}
{{- fail "config.listen must not be empty" -}}
{{- end -}}
{{- if .Values.services -}}
{{- if .Values.networkPolicy.ingress -}}
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
{{- if not (has $field (list "access" "annotations" "networkPolicy" "port" "targetPort" "type")) -}}
{{- fail (printf "services.%s has unknown field %q" $name $field) -}}
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
{{- if not (has $access (list "r" "read-only" "ra" "read-append" "rw" "read-write")) -}}
{{- fail (printf "services.%s.access must be one of r, read-only, ra, read-append, rw, or read-write" $name) -}}
{{- end -}}
{{- $ingressFrom := dig "networkPolicy" "ingressFrom" (list) $service -}}
{{- if not (kindIs "slice" $ingressFrom) -}}
{{- fail (printf "services.%s.networkPolicy.ingressFrom must be a list" $name) -}}
{{- end -}}
{{- if and $.Values.networkPolicy.enabled (gt (len $ingressFrom) 0) -}}
{{- if hasKey $networkPolicyNames $resourceName -}}
{{- fail (printf "services.%s NetworkPolicy name %q conflicts with %s" $name $resourceName (get $networkPolicyNames $resourceName)) -}}
{{- end -}}
{{- $_ := set $networkPolicyNames $resourceName (printf "services.%s" $name) -}}
{{- end -}}
{{- end -}}
{{- else -}}
{{- if not (regexMatch "^[0-9]+$" (include "restic-rados-server.httpPort" .)) -}}
{{- fail (printf "config.listen[0] must be a TCP host:port address, got %v" (first .Values.config.listen)) -}}
{{- end -}}
{{- end -}}
{{- range $name, $repo := .Values.config.repos -}}
{{- if not (or $repo.pools $repo.blob_pools) -}}
{{- fail (printf "repo %q has no pools configured (set config.repos.%s.pools, or remove the repo by setting it to null)" $name $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
