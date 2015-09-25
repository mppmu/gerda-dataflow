#!/bin/bash


#!/bin/bash -e

print_usage() {
echo >&2 "Usage: "`basename "${0}"`" [INPUT] [TIER1_OUTPUT] [TIER1_LOG] [TIERX_OUTPUT] [TIERX_LOG]"
cat >&2 <<EOF
Conversion of GERDA raw data to tier1 and tierX.

Note: Input and output format are preliminary and subject to change!

Options:
  -?                  Show help
  -c                  Conversion method (default: "Struck")
  -p                  Pulse polarity ("normal" or "inverted")

Without -f, ignores input files if the corresponding output file already exists.

Input files may be compressed. For each input file, produces:
    * One "-props.root" file (ROOT file, containing a TTree with pulse properties)
EOF
}


conversion="Struck"
polarity="normal"

while getopts ?c:p: opt
do
	case "$opt" in
		\?)	print_usage; exit 1 ;;
		c) conversion="$OPTARG" ;;
		p) polarity="$OPTARG" ;;
	esac
done
shift `expr $OPTIND - 1`

if [ $# -lt 1 ] ; then print_usage; exit 1; fi

input_file="${1}"
tier1_out="${2}"
tier1_log="${3}"
tierX_out="${4}"
tierX_log="${5}"


raw2mgdo_opts="-m 50"

if [ "${polarity}" = "normal" ] ; then
	true
elif [ "${polarity}" = "inverted" ] ; then
	raw2mgdo_opts="$raw2mgdo_opts --inverted"
else
	echo "ERROR: Illegal value polarity value \"${polarity}\" (option \"-p\")." >&2
	exit 1
fi

cat_util="cat"
if (echo ${input_file} | grep -q '\.gz$') ; then
	cat_util="zcat"
elif (echo ${input_file} | grep -q '\.bz2$') ; then
	cat_util="bzcat"
elif (echo ${input_file} | grep -q '\.xz$') ; then
	cat_util="xzcat"
fi


echo `basename "${0}"` "will ${cat_util} input file \"${input_file}\" to:" >&2
echo "    "Raw2Index -c "${conversion}" -f "${tierX_out}" stdin "&>" "${tierX_log}" >&2
echo "    "Raw2MGDO -c "${conversion}" ${raw2mgdo_opts} -f "${tier1_out}" stdin "&>" "${tier1_log}" >&2

"${cat_util}" "${input_file}" \
	| tee >(Raw2Index -c "${conversion}" -f "${tierX_out}" stdin &> "${tierX_log}") \
	| Raw2MGDO -c "${conversion}" ${raw2mgdo_opts} -f "${tier1_out}" stdin &> "${tier1_log}"

