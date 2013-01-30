/* Generated By:JavaCC: Do not edit this line. SQLParser.java */
package org.dejave.attica.sql.parser;

import java.io.InputStream;
import java.io.ByteArrayInputStream;

import java.util.List;
import java.util.ArrayList;

import org.dejave.attica.server.Statement;
import org.dejave.attica.server.Query;
import org.dejave.attica.server.ShowCatalog;
import org.dejave.attica.server.TableCreation;
import org.dejave.attica.server.TableDeletion;
import org.dejave.attica.server.TupleInsertion;
import org.dejave.attica.server.TableDescription;
import org.dejave.attica.engine.algebra.*;
import org.dejave.attica.model.*;
import org.dejave.attica.storage.Catalog;
import org.dejave.util.Pair;

public class SQLParser implements SQLParserConstants {

       private Catalog catalog;

        public void setCatalog(Catalog cat) {
               this.catalog = cat;
        }

        public static void main(String args[]) {
                System.out.println("Reading from standard input...");
                SQLParser t = new SQLParser(System.in);
                try {
                        t.Start();
                        System.out.println("Thank you.");
                } catch (Exception e) {
                        System.out.println("Oops.");
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                }
        }

/*
TOKEN:
{
	< EMPTY: "" >
}
*/

/****************************************************
 ** The SQL grammar starts from this point forward **
 ****************************************************/
  static final public Statement Start() throws ParseException {
        Object o = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case SELECT:
      o = Query();
                                // safe to ignore the warning since we are
                                // casting to the return type
                                @SuppressWarnings("unchecked")
                                List<AlgebraicOperator> ops =
                                        (List<AlgebraicOperator>) o;
                                {if (true) return new Query(ops);}
      break;
    case CREATE:
      o = Create();
                                {if (true) return new TableCreation((Table) o);}
      break;
    case DROP:
      o = Drop();
                                {if (true) return new TableDeletion((String) o);}
      break;
    case INSERT:
      o = Insert();
                                // safe to ignore the warning since we are
                                // casting to the return type
                                @SuppressWarnings("unchecked")
                                Pair<String, List<Comparable>> pair =
                                     (Pair<String, List<Comparable>>) o;
                                {if (true) return new TupleInsertion(pair.first,
                                                          pair.second);}
      break;
    case CATALOG:
      Catalog();
                                {if (true) return new ShowCatalog();}
      break;
    case DESCRIBE:
      o = Describe();
                                {if (true) return new TableDescription((String) o);}
      break;
    default:
      jj_la1[0] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> Query() throws ParseException {
        List<AlgebraicOperator> algebra = new ArrayList<AlgebraicOperator>();
        List<AlgebraicOperator> where = null;
        Projection p = null;
        Sort s = null;
    p = SelectClause();
                        algebra.add(p);
    FromClause();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case WHERE:
      where = WhereClause();
                                algebra.addAll(where);
      break;
    default:
      jj_la1[1] = jj_gen;
      ;
    }
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ORDER:
      s = SortClause();
                                algebra.add(s);
      break;
    default:
      jj_la1[2] = jj_gen;
      ;
    }
                        {if (true) return algebra;}
    throw new Error("Missing return statement in function");
  }

  static final public Projection SelectClause() throws ParseException {
        List<Variable> projections = new ArrayList<Variable>();
    jj_consume_token(SELECT);
    projections = AttributeList();
                        //System.out.println(projections);
                        Projection p = new Projection(projections);
                        {if (true) return p;}
    throw new Error("Missing return statement in function");
  }

  static final public void FromClause() throws ParseException {
    jj_consume_token(FROM);
    TableList();
  }

  static final public List<AlgebraicOperator> WhereClause() throws ParseException {
        List<AlgebraicOperator> v = null;
    jj_consume_token(WHERE);
    v = BooleanExpression();
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public Sort SortClause() throws ParseException {
        List<Variable> attributes = new ArrayList<Variable>();
    jj_consume_token(ORDER);
    jj_consume_token(BY);
    attributes = AttributeList();
                        Sort s = new Sort(attributes);
                        {if (true) return s;}
    throw new Error("Missing return statement in function");
  }

  static final public List<Variable> AttributeList() throws ParseException {
        List<Variable> v = new ArrayList<Variable>();
        Variable var = null;
    var = Attribute();
                        v.add(var);
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[3] = jj_gen;
        break label_1;
      }
      jj_consume_token(COMMA);
      var = Attribute();
                        v.add(var);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public void TableList() throws ParseException {
    Table();
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[4] = jj_gen;
        break label_2;
      }
      jj_consume_token(COMMA);
      Table();
    }
  }

  static final public String Table() throws ParseException {
        String x = null;
    if (jj_2_1(2147483647)) {
      AliasedTable();
                                {if (true) throw new ParseException("Table aliases not "
                                                         + "yet supported.");}
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ID:
        x = Identifier();
                                {if (true) return x;}
        break;
      default:
        jj_la1[5] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    throw new Error("Missing return statement in function");
  }

  static final public Variable Attribute() throws ParseException {
        Variable var = null;
    if (jj_2_2(2147483647)) {
      var = QualifiedAttribute();
                                {if (true) return var;}
    } else {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case ID:
        Identifier();
                                {if (true) throw new ParseException("Unqualified "
                                                         + "attributes not "
                                                         + "yet supported.");}
        break;
      default:
        jj_la1[6] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> BooleanExpression() throws ParseException {
        List<AlgebraicOperator> v = new ArrayList<AlgebraicOperator>();
    v = DisjunctiveExpression();
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> DisjunctiveExpression() throws ParseException {
        List<AlgebraicOperator> v = new ArrayList<AlgebraicOperator>();
    v = ConjunctiveExpression();
    label_3:
    while (true) {
      if (jj_2_3(2147483647)) {
        ;
      } else {
        break label_3;
      }
      DisjunctionOperator();
                                        {if (true) throw new ParseException("Disjunction "
                                                                 + "not yet "
                                                                 + "supported");}
      ConjunctiveExpression();
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public List<AlgebraicOperator> ConjunctiveExpression() throws ParseException {
        List<AlgebraicOperator> algebra = new ArrayList<AlgebraicOperator>();
        AlgebraicOperator op = null;
    op = UnaryExpression();
                        algebra.add(op);
    label_4:
    while (true) {
      if (jj_2_4(2147483647)) {
        ;
      } else {
        break label_4;
      }
      ConjunctionOperator();
      op = UnaryExpression();
                                        algebra.add(op);
    }
                        {if (true) return algebra;}
    throw new Error("Missing return statement in function");
  }

  static final public AlgebraicOperator UnaryExpression() throws ParseException {
        AlgebraicOperator op = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case NOT:
      NegationOperator();
      BooleanExpression();
                                {if (true) throw new ParseException("Negation not yet "
                                                         + "supported.");}
      break;
    case OPENPAR:
      jj_consume_token(OPENPAR);
      BooleanExpression();
      jj_consume_token(CLOSEPAR);
                                {if (true) throw new ParseException("Nested expressions "
                                                         + "not yet "
                                                         + "supported.");}
      break;
    case ID:
      op = RelationalExpression();
                                {if (true) return op;}
      break;
    default:
      jj_la1[7] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public AlgebraicOperator RelationalExpression() throws ParseException {
        Variable leftVar = null;
        Variable rightVar = null;
        String val = null;
        Qualification.Relationship qual = Qualification.Relationship.EQUALS;
        AlgebraicOperator op = null;
    leftVar = Attribute();
    qual = QualificationOperator();
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case ID:
      rightVar = Attribute();
                                VariableVariableQualification vvarq =
                                        new VariableVariableQualification(qual,
                                            leftVar, rightVar);
                                op = new Join(vvarq);
      break;
    case INTEGER_LITERAL:
    case FLOATING_POINT_LITERAL:
    case STRING_LITERAL:
      val = Literal();
                                VariableValueQualification vvalq =
                                        new VariableValueQualification(qual,
                                            leftVar, val);
                                op = new Selection(vvalq);
      break;
    default:
      jj_la1[8] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
                        {if (true) return op;}
    throw new Error("Missing return statement in function");
  }

  static final public void DisjunctionOperator() throws ParseException {
    jj_consume_token(OR);
  }

  static final public void ConjunctionOperator() throws ParseException {
    jj_consume_token(AND);
  }

  static final public void NegationOperator() throws ParseException {
    jj_consume_token(NOT);
  }

  static final public Qualification.Relationship QualificationOperator() throws ParseException {
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case LESS:
      jj_consume_token(LESS);
                                {if (true) return Qualification.Relationship.LESS;}
      break;
    case LESSEQUAL:
      jj_consume_token(LESSEQUAL);
                                {if (true) return Qualification.Relationship.LESS_EQUALS;}
      break;
    case GREATER:
      jj_consume_token(GREATER);
                                {if (true) return Qualification.Relationship.GREATER;}
      break;
    case GREATEREQUAL:
      jj_consume_token(GREATEREQUAL);
                                {if (true) return Qualification.Relationship.GREATER_EQUALS;}
      break;
    case EQUAL:
      jj_consume_token(EQUAL);
                                {if (true) return Qualification.Relationship.EQUALS;}
      break;
    case NOTEQUAL:
      jj_consume_token(NOTEQUAL);
                                {if (true) return Qualification.Relationship.NOT_EQUALS;}
      break;
    case NOTEQUAL2:
      jj_consume_token(NOTEQUAL2);
                                {if (true) return Qualification.Relationship.NOT_EQUALS;}
      break;
    default:
      jj_la1[9] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public void AliasedTable() throws ParseException {
    jj_consume_token(ID);
    jj_consume_token(ID);
  }

  static final public Variable QualifiedAttribute() throws ParseException {
        Token table = null;
        Token attr = null;
    table = jj_consume_token(ID);
    jj_consume_token(DOT);
    attr = jj_consume_token(ID);
                        {if (true) return new Variable(table.image, attr.image);}
    throw new Error("Missing return statement in function");
  }

  static final public String Identifier() throws ParseException {
        Token x = null;
    x = jj_consume_token(ID);
                        {if (true) return x.image;}
    throw new Error("Missing return statement in function");
  }

  static final public String Literal() throws ParseException {
        Token x = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case STRING_LITERAL:
      x = jj_consume_token(STRING_LITERAL);
                                String s = x.image;
                                s = s.substring(1, s.length()-1);
                                {if (true) return s;}
      break;
    case INTEGER_LITERAL:
      x = jj_consume_token(INTEGER_LITERAL);
                                        {if (true) return x.image;}
      break;
    case FLOATING_POINT_LITERAL:
      x = jj_consume_token(FLOATING_POINT_LITERAL);
                                               {if (true) return x.image;}
      break;
    default:
      jj_la1[10] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public Table Create() throws ParseException {
        List<Attribute> v = null;
        String table;
    jj_consume_token(CREATE);
    jj_consume_token(TABLE);
    table = Identifier();
    jj_consume_token(OPENPAR);
    v = AttributeDeclarationList(table);
    jj_consume_token(CLOSEPAR);
                                {if (true) return new Table(table, v);}
    throw new Error("Missing return statement in function");
  }

  static final public List<Attribute> AttributeDeclarationList(String table) throws ParseException {
        List<Attribute> v = new ArrayList<Attribute>();
        TableAttribute tab = null;
    tab = AttributeDeclaration(table);
                        v.add(tab);
    label_5:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[11] = jj_gen;
        break label_5;
      }
      jj_consume_token(COMMA);
      tab = AttributeDeclaration(table);
                                                v.add(tab);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public TableAttribute AttributeDeclaration(String table) throws ParseException {
        String name = null;
        Class<? extends Comparable> type = null;
    name = Identifier();
    type = Type();
                        {if (true) return new TableAttribute(table, name, type);}
    throw new Error("Missing return statement in function");
  }

  static final public Class<? extends Comparable> Type() throws ParseException {
        Token token = null;
    switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
    case INTEGER:
      jj_consume_token(INTEGER);
                                {if (true) return Integer.class;}
      break;
    case LONG:
      jj_consume_token(LONG);
                                {if (true) return Long.class;}
      break;
    case CHAR:
      jj_consume_token(CHAR);
                                {if (true) return Character.class;}
      break;
    case BYTE:
      jj_consume_token(BYTE);
                                {if (true) return Byte.class;}
      break;
    case SHORT:
      jj_consume_token(SHORT);
                                {if (true) return Short.class;}
      break;
    case DOUBLE:
      jj_consume_token(DOUBLE);
                                {if (true) return Double.class;}
      break;
    case FLOAT:
      jj_consume_token(FLOAT);
                                {if (true) return Float.class;}
      break;
    case STRING:
      jj_consume_token(STRING);
                                {if (true) return String.class;}
      break;
    default:
      jj_la1[12] = jj_gen;
      jj_consume_token(-1);
      throw new ParseException();
    }
    throw new Error("Missing return statement in function");
  }

  static final public Pair<String, List<Comparable>> Insert() throws ParseException {
        String table = null;
        List<Comparable> v = new ArrayList<Comparable>();
    jj_consume_token(INSERT);
    jj_consume_token(INTO);
    table = Identifier();
    jj_consume_token(VALUES);
    jj_consume_token(OPENPAR);
    v = ValueList();
    jj_consume_token(CLOSEPAR);
                        {if (true) return new Pair<String, List<Comparable>>(table, v);}
    throw new Error("Missing return statement in function");
  }

  static final public List<Comparable> ValueList() throws ParseException {
        List<Comparable> v = new ArrayList<Comparable>();
        String l = null;
    l = Literal();
                        v.add(l);
    label_6:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case COMMA:
        ;
        break;
      default:
        jj_la1[13] = jj_gen;
        break label_6;
      }
      jj_consume_token(COMMA);
      l = Literal();
                                v.add(l);
    }
                        {if (true) return v;}
    throw new Error("Missing return statement in function");
  }

  static final public String Drop() throws ParseException {
        String id = null;
    jj_consume_token(DROP);
    jj_consume_token(TABLE);
    id = Identifier();
                        {if (true) return id;}
    throw new Error("Missing return statement in function");
  }

  static final public void Catalog() throws ParseException {
    jj_consume_token(CATALOG);
  }

  static final public String Describe() throws ParseException {
        String s = null;
    jj_consume_token(DESCRIBE);
    jj_consume_token(TABLE);
    s = Identifier();
                        {if (true) return s;}
    throw new Error("Missing return statement in function");
  }

  static final private boolean jj_2_1(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_1(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(0, xla); }
  }

  static final private boolean jj_2_2(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_2(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(1, xla); }
  }

  static final private boolean jj_2_3(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_3(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(2, xla); }
  }

  static final private boolean jj_2_4(int xla) {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    try { return !jj_3_4(); }
    catch(LookaheadSuccess ls) { return true; }
    finally { jj_save(3, xla); }
  }

  static final private boolean jj_3R_7() {
    if (jj_scan_token(ID)) return true;
    if (jj_scan_token(ID)) return true;
    return false;
  }

  static final private boolean jj_3R_8() {
    if (jj_scan_token(ID)) return true;
    if (jj_scan_token(DOT)) return true;
    if (jj_scan_token(ID)) return true;
    return false;
  }

  static final private boolean jj_3_4() {
    if (jj_scan_token(12)) return true;
    return false;
  }

  static final private boolean jj_3_2() {
    if (jj_3R_8()) return true;
    return false;
  }

  static final private boolean jj_3_3() {
    if (jj_scan_token(13)) return true;
    return false;
  }

  static final private boolean jj_3_1() {
    if (jj_3R_7()) return true;
    return false;
  }

  static private boolean jj_initialized_once = false;
  static public SQLParserTokenManager token_source;
  static SimpleCharStream jj_input_stream;
  static public Token token, jj_nt;
  static private int jj_ntk;
  static private Token jj_scanpos, jj_lastpos;
  static private int jj_la;
  static public boolean lookingAhead = false;
  static private boolean jj_semLA;
  static private int jj_gen;
  static final private int[] jj_la1 = new int[14];
  static private int[] jj_la1_0;
  static private int[] jj_la1_1;
  static private int[] jj_la1_2;
  static {
      jj_la1_0();
      jj_la1_1();
      jj_la1_2();
   }
   private static void jj_la1_0() {
      jj_la1_0 = new int[] {0x8000,0x20000,0x40000,0x0,0x0,0x0,0x0,0x4000,0x580,0x0,0x580,0x0,0x0,0x0,};
   }
   private static void jj_la1_1() {
      jj_la1_1 = new int[] {0xe3,0x0,0x0,0x200,0x200,0x0,0x0,0x20000,0x0,0x1fc00,0x0,0x200,0xfe000000,0x200,};
   }
   private static void jj_la1_2() {
      jj_la1_2 = new int[] {0x0,0x0,0x0,0x0,0x0,0x2,0x2,0x2,0x2,0x0,0x0,0x0,0x1,0x0,};
   }
  static final private JJCalls[] jj_2_rtns = new JJCalls[4];
  static private boolean jj_rescan = false;
  static private int jj_gc = 0;

  public SQLParser(java.io.InputStream stream) {
     this(stream, null);
  }
  public SQLParser(java.io.InputStream stream, String encoding) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  You must");
      System.out.println("       either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new SQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  static public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  static public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  public SQLParser(java.io.Reader stream) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  You must");
      System.out.println("       either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    jj_input_stream = new SimpleCharStream(stream, 1, 1);
    token_source = new SQLParserTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  static public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  public SQLParser(SQLParserTokenManager tm) {
    if (jj_initialized_once) {
      System.out.println("ERROR: Second call to constructor of static parser.  You must");
      System.out.println("       either use ReInit() or set the JavaCC option STATIC to false");
      System.out.println("       during parser generation.");
      throw new Error();
    }
    jj_initialized_once = true;
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  public void ReInit(SQLParserTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 14; i++) jj_la1[i] = -1;
    for (int i = 0; i < jj_2_rtns.length; i++) jj_2_rtns[i] = new JJCalls();
  }

  static final private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      if (++jj_gc > 100) {
        jj_gc = 0;
        for (int i = 0; i < jj_2_rtns.length; i++) {
          JJCalls c = jj_2_rtns[i];
          while (c != null) {
            if (c.gen < jj_gen) c.first = null;
            c = c.next;
          }
        }
      }
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }

  static private final class LookaheadSuccess extends java.lang.Error { }
  static final private LookaheadSuccess jj_ls = new LookaheadSuccess();
  static final private boolean jj_scan_token(int kind) {
    if (jj_scanpos == jj_lastpos) {
      jj_la--;
      if (jj_scanpos.next == null) {
        jj_lastpos = jj_scanpos = jj_scanpos.next = token_source.getNextToken();
      } else {
        jj_lastpos = jj_scanpos = jj_scanpos.next;
      }
    } else {
      jj_scanpos = jj_scanpos.next;
    }
    if (jj_rescan) {
      int i = 0; Token tok = token;
      while (tok != null && tok != jj_scanpos) { i++; tok = tok.next; }
      if (tok != null) jj_add_error_token(kind, i);
    }
    if (jj_scanpos.kind != kind) return true;
    if (jj_la == 0 && jj_scanpos == jj_lastpos) throw jj_ls;
    return false;
  }

  static final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

  static final public Token getToken(int index) {
    Token t = lookingAhead ? jj_scanpos : token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  static final private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  static private java.util.Vector<int[]> jj_expentries = new java.util.Vector<int[]>();
  static private int[] jj_expentry;
  static private int jj_kind = -1;
  static private int[] jj_lasttokens = new int[100];
  static private int jj_endpos;

  static private void jj_add_error_token(int kind, int pos) {
    if (pos >= 100) return;
    if (pos == jj_endpos + 1) {
      jj_lasttokens[jj_endpos++] = kind;
    } else if (jj_endpos != 0) {
      jj_expentry = new int[jj_endpos];
      for (int i = 0; i < jj_endpos; i++) {
        jj_expentry[i] = jj_lasttokens[i];
      }
      boolean exists = false;
      for (java.util.Enumeration e = jj_expentries.elements(); e.hasMoreElements();) {
        int[] oldentry = (int[])(e.nextElement());
        if (oldentry.length == jj_expentry.length) {
          exists = true;
          for (int i = 0; i < jj_expentry.length; i++) {
            if (oldentry[i] != jj_expentry[i]) {
              exists = false;
              break;
            }
          }
          if (exists) break;
        }
      }
      if (!exists) jj_expentries.addElement(jj_expentry);
      if (pos != 0) jj_lasttokens[(jj_endpos = pos) - 1] = kind;
    }
  }

  static public ParseException generateParseException() {
    jj_expentries.removeAllElements();
    boolean[] la1tokens = new boolean[68];
    for (int i = 0; i < 68; i++) {
      la1tokens[i] = false;
    }
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 14; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
          if ((jj_la1_1[i] & (1<<j)) != 0) {
            la1tokens[32+j] = true;
          }
          if ((jj_la1_2[i] & (1<<j)) != 0) {
            la1tokens[64+j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 68; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.addElement(jj_expentry);
      }
    }
    jj_endpos = 0;
    jj_rescan_token();
    jj_add_error_token(0, 0);
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = (int[])jj_expentries.elementAt(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  static final public void enable_tracing() {
  }

  static final public void disable_tracing() {
  }

  static final private void jj_rescan_token() {
    jj_rescan = true;
    for (int i = 0; i < 4; i++) {
    try {
      JJCalls p = jj_2_rtns[i];
      do {
        if (p.gen > jj_gen) {
          jj_la = p.arg; jj_lastpos = jj_scanpos = p.first;
          switch (i) {
            case 0: jj_3_1(); break;
            case 1: jj_3_2(); break;
            case 2: jj_3_3(); break;
            case 3: jj_3_4(); break;
          }
        }
        p = p.next;
      } while (p != null);
      } catch(LookaheadSuccess ls) { }
    }
    jj_rescan = false;
  }

  static final private void jj_save(int index, int xla) {
    JJCalls p = jj_2_rtns[index];
    while (p.gen > jj_gen) {
      if (p.next == null) { p = p.next = new JJCalls(); break; }
      p = p.next;
    }
    p.gen = jj_gen + xla - jj_la; p.first = token; p.arg = xla;
  }

  static final class JJCalls {
    int gen;
    Token first;
    int arg;
    JJCalls next;
  }

}
