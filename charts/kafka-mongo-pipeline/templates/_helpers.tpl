{{/*
Expand the name of the chart.
*/}}
{{- define "kafka-mongo-pipeline.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "kafka-mongo-pipeline.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kafka-mongo-pipeline.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ include "kafka-mongo-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kafka-mongo-pipeline.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka-mongo-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
