{{/* Common labels applied to all resources. */}}
{{- define "petri.labels" -}}
app.kubernetes.io/name: petri
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: petri
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* Operator image, tag defaults to chart appVersion. */}}
{{- define "petri.operatorImage" -}}
{{ .Values.operator.image.repository }}:{{ .Values.operator.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/* Deployer image, tag defaults to chart appVersion. */}}
{{- define "petri.deployerImage" -}}
{{ .Values.deployer.image.repository }}:{{ .Values.deployer.image.tag | default .Chart.AppVersion }}
{{- end -}}
