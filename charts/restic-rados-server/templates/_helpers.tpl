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
{{- last (splitList ":" (first .Values.config.listen)) -}}
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
{{- if not .Values.config.listen -}}
{{- fail "config.listen must not be empty" -}}
{{- end -}}
{{- if not (regexMatch "^[0-9]+$" (include "restic-rados-server.httpPort" .)) -}}
{{- fail (printf "config.listen[0] must be a TCP host:port address, got %q" (first .Values.config.listen)) -}}
{{- end -}}
{{- range $name, $repo := .Values.config.repos -}}
{{- if not (or $repo.pools $repo.blob_pools) -}}
{{- fail (printf "repo %q has no pools configured (set config.repos.%s.pools, or remove the repo by setting it to null)" $name $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
