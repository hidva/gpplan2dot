package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"unicode"

	"github.com/awalterschulze/gographviz"
)

func dotEscape(str string) string {
	return strings.ReplaceAll(str, `"`, `\"`)
}

func constructLabel(str ...string) string {
	var newstrs []string
	for _, strval := range str {
		if len(strval) > 0 {
			newstrs = append(newstrs, dotEscape(strval))
		}
	}
	return fmt.Sprintf(`"%s"`, strings.Join(newstrs, `\n`))
}

func SeqScanDotLabel(node *PlanNode) string {
	relname := node.prop["Relation Name"]
	return fmt.Sprintf(`"SeqScan %s\nRows=%d Width=%d"`, relname, uint64(node.rows), node.width)
}

func HashJoinDotLabel(node *PlanNode) string {
	return constructLabel(
		fmt.Sprintf("HashJoin %s", node.prop["Join Type"]),
		node.prop["Hash Cond"].(string),
		getRowWidth(node),
	)
}

func AggregateDotLabel(node *PlanNode) string {
	var props []string
	props = append(props, fmt.Sprintf("%sAggregate", node.prop["Strategy"]))
	groupkeyraw, ok := node.prop["Group Key"]
	if ok {
		for _, keyval := range groupkeyraw.([]interface{}) {
			groupkey := keyval.(string)
			props = append(props, groupkey)
		}
	}
	props = append(props, getRowWidth(node))
	return constructLabel(props...)
}

func getRowWidth(node *PlanNode) string {
	return fmt.Sprintf("Rows=%d Width=%d", uint64(node.rows), node.width)
}

func getStrLabel(show, key string, node *PlanNode) string {
	val := node.GetStrProp(key)
	if len(val) <= 0 {
		return ""
	}
	return fmt.Sprintf("%s=%s", show, val)
}

func IndexScanDotLabel(node *PlanNode) string {
	return constructLabel(
		fmt.Sprintf("IndexScan %s", node.prop["Index Name"]),
		node.prop["Index Cond"].(string),
		getRowWidth(node),
		getStrLabel("Direction", "Scan Direction", node),
		getStrLabel("Relation", "Relation Name", node),
	)
}

func getIntLabel(show, key string, node *PlanNode) string {
	val, ok := node.prop[key]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s=%d", show, int64(val.(float64)))
}

func ForeignScanDotLabel(node *PlanNode) string {
	return constructLabel(
		fmt.Sprintf("ForeignScan %s", node.prop["Relation Name"]),
		getRowWidth(node),
		getIntLabel("OssFdwTotalFiles", "OssFdwTotalFiles", node),
		getIntLabel("OssFdwTotalBytes", "OssFdwTotalBytes", node),
	)
}

func SortDotLabel(node *PlanNode) string {
	sortkey, uniq := node.prop["Sort Key (Distinct)"]
	if !uniq {
		sortkey = node.prop["Sort Key"]
	}
	var props []string
	if uniq {
		props = append(props, "SortDistinct")
	} else {
		props = append(props, "Sort")
	}
	for _, sortkeyraw := range sortkey.([]interface{}) {
		props = append(props, sortkeyraw.(string))
	}
	props = append(props, getRowWidth(node))
	return constructLabel(props...)
}

func UniqueDotLabel(node *PlanNode) string {
	var props []string
	props = append(props, "Unique")
	for _, sortkeyraw := range node.prop["Group Key"].([]interface{}) {
		props = append(props, sortkeyraw.(string))
	}
	props = append(props, getRowWidth(node))
	return constructLabel(props...)
}

var g_getdotlabel = map[string]func(node *PlanNode) string{
	"SeqScan":     SeqScanDotLabel,
	"HashJoin":    HashJoinDotLabel,
	"Aggregate":   AggregateDotLabel,
	"ForeignScan": ForeignScanDotLabel,
	"IndexScan":   IndexScanDotLabel,
	"Sort":        SortDotLabel,
	"Unique":      UniqueDotLabel,
}

type PlanNode struct {
	nodeType string
	prop     map[string]interface{}
	children []*PlanNode
	parent   *PlanNode
	slice    *Slice
	rows     float64
	width    int
	id       int
}

func (this *PlanNode) GetStrProp(key string) string {
	val, ok := this.prop[key]
	if !ok {
		return ""
	}
	return val.(string)
}

// "gather motion" -> "GatherMotion"
func formatName(n string) string {
	n = strings.TrimSpace(n)
	input := []rune(n)
	var output []rune
	upperit := true
	for _, ch := range input {
		if unicode.IsSpace(ch) {
			upperit = true
			continue
		}
		if upperit {
			ch = unicode.ToUpper(ch)
			upperit = false
		}
		output = append(output, ch)
	}
	return string(output)
}

func (this *PlanNode) Init(plan map[string]interface{}) {
	this.nodeType = formatName(plan["Node Type"].(string))
	this.rows = plan["Plan Rows"].(float64)
	this.width = int(plan["Plan Width"].(float64))
	this.prop = plan
}

func (this *PlanNode) IsMotion() bool {
	return strings.Contains(this.nodeType, "Motion")
}

func (this *PlanNode) DotName() string {
	return fmt.Sprintf("Plan%d", this.id)
}

func (this *PlanNode) DotLabel() string {
	f, ok := g_getdotlabel[this.nodeType]
	if ok {
		return f(this)
	}
	nodename := this.nodeType
	if this.IsMotion() {
		sendernum := this.children[0].slice.gangsize
		recvnum := 1
		if this.parent != nil {
			recvnum = this.parent.slice.gangsize
		}
		nodename = fmt.Sprintf("%s %d→%d", nodename, sendernum, recvnum)
	}
	return fmt.Sprintf(`"%s\nRows=%d Width=%d"`, nodename, uint64(this.rows), this.width)
}

type Slice struct {
	gangtype string
	children []*Slice
	parent   *Slice
	root     *PlanNode
	gangsize int
	sliceid  int
	id       int
}

func (this *Slice) Init(data map[string]interface{}) {
	this.gangsize = int(data["Senders"].(float64))
	this.gangtype = formatName(data["Gang Type"].(string))
	this.sliceid = int(data["Slice"].(float64))
}

func (this *Slice) DotName() string {
	return fmt.Sprintf("cluster_%d", this.sliceid)
}

func (this *Slice) DotLabel() string {
	return fmt.Sprintf(`"slice%d\n(size=%d type=%s)"`, this.sliceid, this.gangsize, this.gangtype)
}

func TraverseMotion(nextid *int, recvSlice *Slice, motion *PlanNode, sendSliceData map[string]interface{}) {
	motion.slice = nil
	newPlan := &PlanNode{
		parent: motion,
		id:     *nextid,
	}
	(*nextid)++
	sendSlice := &Slice{
		id:     *nextid,
		parent: recvSlice,
		root:   newPlan,
	}
	(*nextid)++
	newPlan.slice = sendSlice
	sendSlice.Init(sendSliceData)
	recvSlice.children = append(recvSlice.children, sendSlice)
	motion.children = append(motion.children, newPlan)

	rawChlidPlans, ok := sendSliceData["Plans"]
	if !ok {
		panic(fmt.Errorf("No child plan for motion"))
	}
	childplans := rawChlidPlans.([]interface{})
	if len(childplans) != 1 {
		panic(fmt.Errorf("Expect only one child plan for motion"))
	}
	TraversePlan(nextid, sendSlice, newPlan, childplans[0].(map[string]interface{}))
}

func TraversePlan(nextid *int, curSlice *Slice, curPlan *PlanNode, plan map[string]interface{}) {
	curPlan.Init(plan)
	if curPlan.IsMotion() {
		TraverseMotion(nextid, curSlice, curPlan, plan)
		return
	}
	childplans, ok := plan["Plans"]
	if !ok {
		return
	}
	for _, rawchildplan := range childplans.([]interface{}) {
		childplanval := rawchildplan.(map[string]interface{})
		childplan := &PlanNode{
			parent: curPlan,
			id:     *nextid,
			slice:  curSlice,
		}
		(*nextid)++
		curPlan.children = append(curPlan.children, childplan)
		TraversePlan(nextid, curSlice, childplan, childplanval)
	}
}

func Parse(queryPlan map[string]interface{}) *PlanNode {
	plan, ok := queryPlan["Plan"]
	if !ok {
		panic(fmt.Errorf("Fail to get plan of query"))
	}
	nextid := 0
	rootSlice := &Slice{
		gangtype: "Unallocated",
		gangsize: 1,
		id:       nextid,
		root:     &PlanNode{id: nextid + 1},
	}
	nextid += 2
	rootSlice.root.slice = rootSlice
	TraversePlan(&nextid, rootSlice, rootSlice.root, plan.(map[string]interface{}))
	return rootSlice.root
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func AddPlan(g *gographviz.Graph, plan *PlanNode) {
	graphname := g.Name
	if plan.slice != nil {
		// Assert !plan.IsMotion()
		if !g.IsSubGraph(plan.slice.DotName()) {
			must(g.AddSubGraph(g.Name, plan.slice.DotName(), map[string]string{
				"label": plan.slice.DotLabel(),
				"style": "filled",
				"color": "pink",
			}))
		}
		graphname = plan.slice.DotName()
	}
	must(g.AddNode(graphname, plan.DotName(), map[string]string{
		"label":     plan.DotLabel(),
		"shape":     "box",
		"style":     "filled",
		"fillcolor": "black",
		"fontcolor": "white",
	}))
	for _, child := range plan.children {
		AddPlan(g, child)
		must(g.AddEdge(child.DotName(), plan.DotName(), true, map[string]string{}))
	}
}

func ToDot(root *PlanNode) string {
	astG, err := gographviz.ParseString(`strict digraph G {
		rankdir=BT;
		label="Query Plan - Generated By hidva/gpplan2dot";
	}`)
	must(err)
	g, err := gographviz.NewAnalysedGraph(astG)
	must(err)
	AddPlan(g, root)
	return g.String()
}

func main() {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
	var plan interface{}
	if err = json.Unmarshal(data, &plan); err != nil {
		panic(err)
	}

	fmt.Println(ToDot(Parse(((plan.([]interface{}))[0]).(map[string]interface{}))))
}
