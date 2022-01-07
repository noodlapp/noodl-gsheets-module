const Noodl = require('@noodl/noodl-sdk');
const PublicGoogleSheetsParser = require('./sheetparser')
const EventEmitter = require('events').EventEmitter

var _schemas,_schemaEvents

function _addSheetToSchemas(id,sheet,cb) {
	const parser = new PublicGoogleSheetsParser(id,sheet,"limit 0")

	parser.parse().then(({rows,cols}) => {
		const schema = {}
		const _types = {
			"boolean":{type:"Boolean"},
			"string":{type:"String"},
			"number":{type:"Number"},
			"date":{type:"Date"}
		}

		for(var i = 0; i < cols.length; i++) {
			const key = cols[i].label
			schema[key] = _types[cols[i].type]
		}

		_schemas[sheet||'_Default'] = Object.assign(_schemas[sheet||'_Default']||{},schema)
		_schemaEvents.emit('change',{schema:sheet})
		cb()
	})
}

const QuerySheetNode = Noodl.defineNode({
	name:'noodl.gsheets.QuerySheetNode',
	displayName:'Query Sheet',
	color:'green',
	inputs:{
		sheetId:{displayName:'Document Id',group:'Sheet Source',type:'string'},
		sheetName:{displayName:'Sheet Name',group:'Sheet Source',type:'string'},
	},
	outputs:{
		result:{displayName:'Items',group:'General',type:'array'},
		count:{displayName:'Count',group:'General',type:'array'},
		firstItemId:{displayName:'First Item Id',group:'General',type:'array'}
	},
	changed:{	
		sheetId() {
			this.cols = undefined
			this.scheduleQuery()
		},
		sheetName() {
			this.cols = undefined
			this.scheduleQuery()
		}
	},
	methods:{
		scheduleQuery() {
			if(this.queryScheduled) return
			this.queryScheduled = true
			this.scheduleAfterInputsHaveUpdated(() => {
				this.queryScheduled = false
				this.runQuery()
			})
		},
	 	_formatFilter(query,options) {
			var inputs = options.queryParameters;
	   
		   if(query.combinator !== undefined && query.rules !== undefined) {
			   if(query.rules.length === 0) return;
			   else if(query.rules.length === 1) return this._formatFilter(query.rules[0],options)
			   else {
				   const _res = '('
				   query.rules.forEach((r,idx) => {
					   var cond = this._formatFilter(r,options)
					   if(cond !== undefined) _res += cond
					   if(idx < query.rules.length-1) _res += query.combinator
				   })
				   _res += ')'
	   
				   return _res;
			   }
		   }
		   else {
			   var cond;
			   var value = query.input!==undefined?inputs[query.input]:query.value;

			   if(typeof value === 'string') value = "'" + value + "'"
 	   
			   if(query.operator === 'exist') cond = 'is not null'
			   else if(query.operator === 'not exist') cond = 'is null'
			   else if(query.operator === 'greater than') cond = '> ' + value
			   else if(query.operator === 'greater than or equal to') cond = '>= ' + value
			   else if(query.operator === 'less than') cond = '< ' + value
			   else if(query.operator === 'less than or equal to') cond = '<= ' + value
			   else if(query.operator === 'equal to') cond = '= ' + value
			   else if(query.operator === 'not equal to') cond = '!= ' + value
	   
			   const _res = options.cols[query.property].id + " " + cond
	   
			   return _res;
		   }
		},
		_getColumns(cb) {
			if(this.cols !== undefined) return cb(this.cols)
			const parser = new PublicGoogleSheetsParser(this.inputs.sheetId,this.inputs.sheetName,"limit 0")
			parser.parse().then(({rows,cols}) => {
				this.cols = {}
				cols.forEach(c => this.cols[c.label] = c)
				cb(this.cols)
			})
		},
		runQuery() {
			this._getColumns((cols) => {
				// Generate the query from the visual filter
				let query
				if(this.filter !== undefined) {
					const filter = this._formatFilter(this.filter,{cols})
					if(filter !== undefined) query = 'where ' + filter + ' '
				}

				if(this.sorting !== undefined) {
					 query = (query||'') + 'order by ' + this.sorting.map((s,idx) => {
						return  (idx !== 0?' ':'') + cols[s.property].id + (s.order === 'descending'?' desc':'')
					}).join(' ') + ' '
				}

				if(this.enableLimit) {
					query = (query||'') + 'limit ' + (this.limit!==undefined?this.limit:10) + ' '
					if(this.skip !== undefined) query = (query||'') + 'offset ' + this.skip + ' '
				}

				const parser = new PublicGoogleSheetsParser(this.inputs.sheetId,this.inputs.sheetName,query)
				parser.parse().then(({rows,cols}) => {
					const results = Noodl.Array.get()
					results.set(rows.map((r) => {
						let _id
						if(this.useColumnForId !== undefined && this.useColumnForId !== '__none__') {
							_id = r[this.useColumnForId]
							delete r[this.useColumnForId]
						}
						let obj = Noodl.Object.get(_id)
						obj.setAll(r)
						return obj
					}))

					this.setOutputs({
						result:results,
						count:results.size(),
						firstItemId:(results.size() > 0)?results.get(0).getId():undefined
					})
				})
			})
		},
		registerInputIfNeeded: function (name) {
			if (this.hasInput(name)) {
				return;
			}

			if(name === 'visualFilter') this.registerInput(name, {
				set: this.setFilter.bind(this)
			})

			if(name === 'visualSort') this.registerInput(name, {
				set: this.setSorting.bind(this)
			})

			if(name === 'enableLimit') this.registerInput(name, {
				set: this.setEnableLimit.bind(this)
			})

			if(name === 'limit') this.registerInput(name, {
				set: this.setLimit.bind(this)
			})

			if(name === 'skip') this.registerInput(name, {
				set: this.setSkip.bind(this)
			})

			if(name === 'columnForId') this.registerInput(name, {
				set: this.setColumnForId.bind(this)
			})
		},		
		setFilter(value) {
			this.filter = value
			this.scheduleQuery()
		},
		setSorting(value) {
			this.sorting = value
			this.scheduleQuery()
		},
		setEnableLimit(value) {
			this.enableLimit = value
			this.scheduleQuery()
		},
		setLimit(value) {
			this.limit = value
			this.scheduleQuery()
		},
		setSkip(value) {
			this.skip = value
			this.scheduleQuery()
		},
		setColumnForId(value) {
			this.useColumnForId = value
			this.scheduleQuery()
		}
	},
	setup: function (context, graphModel) {
		if (!context.editorConnection || !context.editorConnection.isRunningLocally()) {
			return;
		}

		if(_schemas === undefined) _schemas = {}
		if(_schemaEvents === undefined) _schemaEvents = new EventEmitter()

		graphModel.on("nodeAdded.noodl.gsheets.QuerySheetNode", function (node) {

			function updatePorts() {
				var ports = []

				// Limit
				ports.push({
					type: 'boolean',
					plug: 'input',
					group: 'Limit',
					name: 'enableLimit',
					displayName: 'Use limit',
				})
			
				if (node.parameters['enableLimit']) {
					ports.push({
						type: 'number',
						default: 10,
						plug: 'input',
						group: 'Limit',
						name: 'limit',
						displayName: 'Limit',
					})
			
					ports.push({
						type: 'number',
						default: 0,
						plug: 'input',
						group: 'Limit',
						name: 'skip',
						displayName: 'Skip',
					})
				}

				const schema = {properties:_schemas[node.parameters.sheetName||'_Default']}

				ports.push({
                    name:'visualFilter',
                    plug:'input',
                    type:{name:'query-filter',schema:schema,allowEditOnly:true},
                    displayName:'Filter',
                    group:'Filter',
                })

                ports.push({
                    name:'visualSort',
                    plug:'input',
                    type:{name:'query-sorting',schema:schema,allowEditOnly:true},
                    displayName:'Sort',
                    group:'Sorting',
                })

				const columnForIdEnums = [{label:'Unique id',value:'__none__'}].concat(Object.keys(schema.properties).map(k => ({value:k,label:k})))
				ports.push({
					name:'columnForId',
					plug:'input',
					type:{name:'enum',enums:columnForIdEnums},
					displayName:'Use Column For Id',
					group:'Sheet Source',
					default:'__none__'
				})

				context.editorConnection.sendDynamicPorts(node.id, ports);
			}

			if(node.parameters.sheetId) {
				_addSheetToSchemas(node.parameters.sheetId,node.parameters.sheetName,() => {
					updatePorts();
				})
			}

			node.on("parameterUpdated", function (event) {
				if ((event.name === "sheetId" || event.name === "sheetName") && node.parameters.sheetId) {
					_addSheetToSchemas(node.parameters.sheetId,node.parameters.sheetName,() => {
						updatePorts();
					})
				}

				if(event.name === "enableLimit") updatePorts();
			})
		})
	}
})

const QuerySheetUniqueColumnNode = Noodl.defineNode({
	name:'noodl.gsheets.QuerySheetUniqueColumnNode',
	displayName:'Query Sheet Unique',
	color:'green',
	inputs:{
		sheetId:{displayName:'Document Id',group:'Sheet Source',type:'string'},
		sheetName:{displayName:'Sheet Name',group:'Sheet Source',type:'string'},
	},
	outputs:{
		result:{displayName:'Items',group:'General',type:'array'},
		count:{displayName:'Count',group:'General',type:'array'},
		firstItemId:{displayName:'First Item Id',group:'General',type:'array'}
	},
	changed:{	
		sheetId() {
			this.cols = undefined
			this.scheduleQuery()
		},
		sheetName() {
			this.cols = undefined
			this.scheduleQuery()
		}
	},
	methods:{
		scheduleQuery() {
			if(this.queryScheduled) return
			this.queryScheduled = true
			this.scheduleAfterInputsHaveUpdated(() => {
				this.queryScheduled = false
				this.runQuery()
			})
		},
		_getColumns(cb) {
			if(this.cols !== undefined) return cb(this.cols)
			const parser = new PublicGoogleSheetsParser(this.inputs.sheetId,this.inputs.sheetName,"limit 0")
			parser.parse().then(({rows,cols}) => {
				this.cols = {}
				cols.forEach(c => this.cols[c.label] = c)
				cb(this.cols)
			})
		},
		runQuery() {
			this._getColumns((cols) => {
				const col = cols[this.column].id
				const parser = new PublicGoogleSheetsParser(this.inputs.sheetId,this.inputs.sheetName,`select ${col}, count(${col}) group by ${col}`)
				parser.parse().then(({rows,cols}) => {
					const results = Noodl.Array.get()
					results.set(rows.map((r) => {
						let obj = Noodl.Object.create({Value:r[this.column]})
						return obj
					}))
	
					this.setOutputs({
						result:results,
						count:results.size(),
						firstItemId:(results.size() > 0)?results.get(0).getId():undefined
					})
				})
			})
		},
		registerInputIfNeeded: function (name) {
			if (this.hasInput(name)) {
				return;
			}

			if(name === 'column') this.registerInput(name, {
				set: this.setColumn.bind(this)
			})
		},		
		setColumn(value) {
			this.column = value
			this.scheduleQuery()
		}
	},
	setup: function (context, graphModel) {
		if (!context.editorConnection || !context.editorConnection.isRunningLocally()) {
			return;
		}

		if(_schemas === undefined) _schemas = {}
		if(_schemaEvents === undefined) _schemaEvents = new EventEmitter()

		graphModel.on("nodeAdded.noodl.gsheets.QuerySheetUniqueColumnNode", function (node) {

			function updatePorts() {
				var ports = []

				const schema = {properties:_schemas[node.parameters.sheetName||'_Default']}

				const columnForIdEnums = Object.keys(schema.properties).map(k => ({value:k,label:k}))
				if(columnForIdEnums.length > 0) {
					ports.push({
						name:'column',
						plug:'input',
						type:{name:'enum',enums:columnForIdEnums},
						displayName:'Column',
						group:'Sheet Source',
						default:columnForIdEnums[0].value
					})
				}

				context.editorConnection.sendDynamicPorts(node.id, ports);
			}

			if(node.parameters.sheetId) {
				_addSheetToSchemas(node.parameters.sheetId,node.parameters.sheetName,() => {
					updatePorts();
				})
			}

			node.on("parameterUpdated", function (event) {
				if ((event.name === "sheetId" || event.name === "sheetName") && node.parameters.sheetId) {
					_addSheetToSchemas(node.parameters.sheetId,node.parameters.sheetName,() => {
						updatePorts();
					})
				}
			})
		})
	}
})

const SheetRowNode = Noodl.defineNode({
	name:'noodl.gsheets.SheetRowNode',
	displayName:'Sheet Row',
	color:'green',
	inputs:{
		rowId:{type:'string',displayName:'Row Id',allowConnectionsOnly:true}
	},
	outputs:{
	},
	changed:{	
		rowId(value) {
			this.rowObject = Noodl.Object.get(value)
			this.updateOutputs()
		}
	},
	methods:{
		registerOutputIfNeeded(name) {
			if (this.hasOutput(name)) {
				return;
			}

			if (name.startsWith('prop-')) this.registerOutput(name, {
                getter: this.getColumnValue.bind(this, name.substring('prop-'.length))
            })
		},	
		getColumnValue(name) {
			if(this.rowObject === undefined) return
			return this.rowObject.get(name)
		},
		updateOutputs() {
			if(this.rowObject === undefined) return
			const out = {}
			Object.keys(this.rowObject.data).forEach(k => out['prop-'+k] = this.rowObject[k])
			this.setOutputs(out)
		}
	},
	setup: function (context, graphModel) {
		if (!context.editorConnection || !context.editorConnection.isRunningLocally()) {
			return;
		}

		if(_schemas === undefined) _schemas = {}
		if(_schemaEvents === undefined) _schemaEvents = new EventEmitter()

		graphModel.on("nodeAdded.noodl.gsheets.SheetRowNode", function (node) {

			function updatePorts() {
				var ports = []

				const sheets = Object.keys(_schemas).map(k => ({value:k,label:k}))
				if(sheets.length > 1) {
					ports.push({
						name:'sheet',
						plug:'input',
						type:{name:'enum',enums:sheets},
						displayName:'Sheet',
						group:'General',
						default:sheets[0].value
					})
				}

				const schema = _schemas[node.parameters['sheet'] || '_Default']
				if(schema !== undefined) {
					Object.keys(schema).forEach(prop => {
						const type = schema[prop].type
						const _types = {
							"Boolean":"boolean",
							"Number":"number",
							"String":"string"
						}
						ports.push({
							name:'prop-'+prop,
							plug:'output',
							type:_types[type]||'*',
							displayName:prop,
							group:'Columns'
						})
					})
				}

				context.editorConnection.sendDynamicPorts(node.id, ports);
			}

			updatePorts()
			_schemaEvents.on('change',() => {
				updatePorts()
			})
		})
	}
})

Noodl.defineModule({
    nodes:[
		QuerySheetNode,
		QuerySheetUniqueColumnNode,
		SheetRowNode
    ],
    setup() {
    	//this is called once on startup
    }
});