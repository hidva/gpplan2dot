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

func SeqScanDotLabel(node *PlanNode) string {
	relname := node.prop["Relation Name"]
	return fmt.Sprintf(`"SeqScan %s\nRows=%d Width=%d"`, relname, uint64(node.rows), node.width)
}

func HashJoinDotLabel(node *PlanNode) string {
	jointype := node.prop["Join Type"]
	return fmt.Sprintf(`"HashJoin %s\nRows=%d Width=%d"`, jointype, uint64(node.rows), node.width)
}

func AggregateDotLabel(node *PlanNode) string {
	strategy := node.prop["Strategy"]
	return fmt.Sprintf(`"%sAggregate\nRows=%d Width=%d"`, strategy, uint64(node.rows), node.width)
}

func fillInt64Prop(props []string, nodeprops map[string]interface{}, key string) []string {
	if val, ok := nodeprops[key]; ok {
		return append(props, fmt.Sprintf("%s=%d", key, int64(val.(float64))))
	}
	return props
}

func ForeignScanDotLabel(node *PlanNode) string {
	relname := node.prop["Relation Name"]
	var props []string
	props = fillInt64Prop(props, node.prop, "OssFdwCsvMaxParallel")
	props = fillInt64Prop(props, node.prop, "OssFdwTotalFiles")
	props = fillInt64Prop(props, node.prop, "OssFdwTotalBytes")
	if len(props) <= 0 {
		return fmt.Sprintf(`"ForeignScan %s\nRows=%d Width=%d"`, relname, uint64(node.rows), node.width)
	}
	return fmt.Sprintf(`"ForeignScan %s\nRows=%d Width=%d\n%s"`, relname, uint64(node.rows), node.width, strings.Join(props, `\n`))
}

var g_getdotlabel = map[string]func(node *PlanNode) string{
	"SeqScan":     SeqScanDotLabel,
	"HashJoin":    HashJoinDotLabel,
	"Aggregate":   AggregateDotLabel,
	"ForeignScan": ForeignScanDotLabel,
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
